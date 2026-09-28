// Copyright 2026 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package planner

import (
	"cmp"
	"slices"

	"github.com/sourcenetwork/immutable"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/request"
	"github.com/sourcenetwork/defradb/errors"
	"github.com/sourcenetwork/defradb/internal/datastore"
	"github.com/sourcenetwork/defradb/internal/db/fetcher"
	"github.com/sourcenetwork/defradb/internal/db/id"
	"github.com/sourcenetwork/defradb/internal/db/vectorindex"
	"github.com/sourcenetwork/defradb/internal/extensions"
	"github.com/sourcenetwork/defradb/internal/keys"
	"github.com/sourcenetwork/defradb/internal/planner/mapper"
)

// Sent as the `reason` detail of a [client.WarningCodeVectorIndexUnused] warning. Callers match on
// them, so they do not change once released.
const (
	reasonNoLimit = "noLimit"
	// The filter names something the scan cannot check on its own: a related document, an alias, or
	// a field lens migrations still have to transform. Only the query's shape decides this.
	reasonFilter = "filter"
	// The filter passed fewer than limit+offset of the nearest documents the index was allowed to
	// examine, so the whole collection was read to find the rest.
	reasonFilterTooSelective         = "filterTooSelective"
	reasonNotOrderedBySimilarityDesc = "notOrderedBySimilarityDesc"
	// The index holds only documents that are not deleted, so it cannot find the deleted ones the
	// query asks for too.
	reasonShowDeleted = "showDeleted"
	// The query selects a relation's documents, which the join narrows to each parent's only when it
	// runs, so the collection's nearest documents are not the ones asked for.
	reasonNestedSelect = "nestedSelect"
	// The limit counts groups, not documents, so the nearest documents may fill fewer of them.
	reasonGroupBy = "groupBy"
	// Lifting this is https://github.com/sourcenetwork/defradb/issues/5072
	reasonMultipleSimilarityFields = "multipleSimilarityFields"
)

// How a nearest-neighbour query was answered. Reported by `_explain(type: execute)` as the scan's
// `vectorStrategy`, so it can be seen which one ran.
const (
	// The index returned the nearest documents and every one of them is in the result.
	vectorStrategyGraph = "graph"
	// The index was searched in growing batches until enough of the nearest documents passed the
	// filter (or, with maxCandidates, until the cap was reached).
	vectorStrategyOverFetch = "overFetch"
	// A secondary index resolved the filter and every matching document was scored exactly. The
	// vector index may have been searched first, until the secondary index proved the cheaper way or
	// the search reached its budget.
	vectorStrategyFilterIndex = "filterIndex"
)

// When the filter has a secondary index, there are two ways to answer: score every document the
// filter matches (M fetches, exact), or search the vector index until k pass (about k*N/M fetches at
// selectivity M/N). The first is cheaper while M < sqrt(k*N), but M and N are not known up front and
// there are no statistics to estimate them. So both run side by side: after each over-fetch round,
// the secondary index is read for as many matches as that round examined candidates, and whichever
// finishes first answers. That costs at most about twice the cheaper way, whatever the selectivity or
// the collection size, where a fixed match threshold would be wrong for most of them.

// The over-fetch search asks the index for overFetchFirstBatchFactor*k documents, then multiplies
// the batch by overFetchGrowthFactor each round until k pass the filter or the budget is reached.
//
// A filter that passes half the documents usually fills k in the first round. Doubling keeps the
// rounds few: each round searches the graph afresh, so the total work is about twice the last batch,
// never the sum of many small ones.
const (
	overFetchFirstBatchFactor = 2
	overFetchGrowthFactor     = 2
)

// Without maxCandidates, the over-fetch search examines at most the larger of
// overFetchBudgetFactor*k and overFetchMinBudget documents before giving up: the filter's secondary
// index then scores every match, and without one the whole collection is read.
//
// Examining a candidate costs a graph search share plus a fetch, and the doubling rounds roughly
// triple that, so past a certain point the full scan's single pass is the cheaper, more predictable
// way. With these values the search succeeds for filters passing at least 1 in 64 of the nearest
// documents, or 1% for a limit of 10. Rarer filters are the ones a secondary index serves exactly
// when the filtered field has one; without one, the full scan is the only way to know all of them
// anyway. The minimum keeps a small limit from giving up after a handful of documents.
const (
	overFetchBudgetFactor = 64
	overFetchMinBudget    = 1024
)

// warnCandidateLimitReached reports that a filtered nearest-neighbour query stopped at the
// maxCandidates the caller set, so its result can be missing matching documents.
//
// The details repeat only the query's own values. Nothing counted from the documents examined goes
// here: those include documents the caller may not be allowed to see.
func (n *selectNode) warnCandidateLimitReached(sim *mapper.Similarity, maxCandidates int) {
	fieldName := sim.SimilarityTarget.Field.Name
	extensions.AddWarning(n.planner.ctx, client.GQLWarning{
		Code: client.WarningCodeVectorCandidateLimitReached,
		Message: "similarity query on field '" + fieldName + "' returned fewer documents than its " +
			"limit because the vector index stopped at maxCandidates; more matching documents may " +
			"exist, so raise maxCandidates, leave it out, or narrow the filter",
		Detail: map[string]any{
			"field":         fieldName,
			"limit":         n.selectReq.Limit.Limit,
			"maxCandidates": maxCandidates,
		},
	})
}

// warnVectorIndexUnused reports that the query reads the whole collection even though the field has
// a vector index. The results are still correct, so this is a warning and not an error.
func (n *selectNode) warnVectorIndexUnused(sim *mapper.Similarity, reason string) {
	fieldName := sim.SimilarityTarget.Field.Name
	extensions.AddWarning(n.planner.ctx, client.GQLWarning{
		Code: client.WarningCodeVectorIndexUnused,
		Message: "similarity query on field '" + fieldName +
			"' did not use the vector index and read the whole collection",
		Detail: map[string]any{
			"field":  fieldName,
			"reason": reason,
		},
	})
}

// tryRouteSimilarityToVectorIndex narrows an otherwise-full scan to the k nearest documents when the
// query is a nearest-neighbour search (a single `_similarity` ordered descending, with a limit) and
// the field has a ready vector index. It feeds the graph search results to the scan as document
// prefixes; the similarity/order/limit nodes are left to score, sort and cap as usual. When the query
// does not match, it leaves the full-scan path in place.
func (n *selectNode) tryRouteSimilarityToVectorIndex(origScan *scanNode) error {
	// The index is looked up before the query shape is checked. Without one there is nothing to fall
	// back from, so warning would be noise.
	sims := n.similarityFields()
	if len(sims) == 0 {
		return nil
	}
	if len(sims) > 1 {
		// Which one drives the search is ambiguous, so the query full-scans. Letting the query say
		// which it means is https://github.com/sourcenetwork/defradb/issues/5072
		//
		// Reported against the first field that has an index, since the warning is one per query.
		for _, sim := range sims {
			if _, ok := n.readyVectorIndexOnField(sim.SimilarityTarget.Field.Name); ok {
				n.warnVectorIndexUnused(sim, reasonMultipleSimilarityFields)
				return nil
			}
		}
		return nil
	}
	sim := sims[0]

	index, ok := n.readyVectorIndexOnField(sim.SimilarityTarget.Field.Name)
	if !ok {
		return nil
	}

	if n.planner.relatedSelectDepth > 0 {
		n.warnVectorIndexUnused(sim, reasonNestedSelect)
		return nil
	}
	if n.selectReq.GroupBy != nil {
		n.warnVectorIndexUnused(sim, reasonGroupBy)
		return nil
	}
	if n.selectReq.ShowDeleted {
		n.warnVectorIndexUnused(sim, reasonShowDeleted)
		return nil
	}
	if n.selectReq.Limit == nil || n.selectReq.Limit.Limit <= 0 {
		n.warnVectorIndexUnused(sim, reasonNoLimit)
		return nil
	}
	if !n.isOrderedBySimilarityDesc(sim) {
		n.warnVectorIndexUnused(sim, reasonNotOrderedBySimilarityDesc)
		return nil
	}
	// readyVectorIndexOnField only returns a vector index, so GetVector always succeeds here.
	vectorDesc, _ := index.GetVector()

	// No warning here: the vector is malformed rather than the query shape being wrong, so there is
	// nothing to rewrite. The full-scan path reports its own error.
	query, ok := similarityQueryVector(sim.Vector)
	if !ok {
		return nil
	}

	// A wrong-length query would be scored on only its shared leading elements, giving wrong results.
	// The full-scan path errors on this; do the same here.
	if dims := int(vectorDesc.Dimensions); dims > 0 && len(query) != dims {
		return NewErrMismatchLengthOnSimilarity(dims, len(query))
	}

	// Offset skips the first documents of the result, so the graph must return them too, not just the
	// Limit that remains after skipping. Ask for Limit+Offset; the limit node applies the offset.
	k := int(n.selectReq.Limit.Limit) + int(n.selectReq.Limit.Offset)

	// Read the filter from the scan, not n.filter: initSource moves it there and leaves n.filter nil
	// unless the collection has migrations.
	if origScan.filter == nil && !n.hidesDocuments() {
		// Every one of the k nearest documents is in the result, so the graph answers alone.
		results, _, err := n.vectorSearch(index, query, k)
		if err != nil {
			return err
		}
		docs, err := n.docPrefixes(results)
		if err != nil {
			return err
		}
		n.useVectorPrefixes(origScan, vectorStrategyGraph, prefixesOf(docs), 1)
		return nil
	}

	// Some of the nearest documents may be left out: the filter rejects them, or access control hides
	// them from the caller. Either way the graph's k nearest are not the answer, so check each one the
	// way the scan will before relying on it.
	if origScan.filter != nil && !n.canCheckFilterAtScan(origScan) {
		n.warnVectorIndexUnused(sim, reasonFilter)
		return nil
	}

	budget := max(overFetchBudgetFactor*k, overFetchMinBudget)
	maxCandidates := 0
	if sim.MaxCandidates.HasValue() {
		// Fewer candidates than the result needs could never fill it, so the cap is raised to that.
		maxCandidates = max(int(sim.MaxCandidates.Value()), k)
		budget = maxCandidates
	}

	target := sim.SimilarityTarget.Field.Name
	filterIndex := n.filterIndex(origScan)
	var probe *filterIndexProbe
	if filterIndex.HasValue() {
		var err error
		probe, err = n.startFilterIndexProbe(origScan, target, filterIndex.Value())
		if err != nil {
			return err
		}
	}

	found, err := n.overFetch(origScan, target, index, query, k, budget, probe)
	if probe != nil {
		err = errors.Join(err, probe.close())
	}
	if err != nil {
		return err
	}

	// Counted in explain whichever way the query is answered, since they ran either way.
	origScan.vectorSearches = found.searches

	if len(found.passing) >= k {
		n.useVectorPrefixes(origScan, vectorStrategyOverFetch, prefixesOf(nearest(found.passing, k)), found.searches)
		return nil
	}
	if probe != nil && probe.done {
		// Every match was read in no more reads than the search had spent, so scoring them all is the
		// cheaper way, and it is exact. selectIndex picks this same index for the scan below.
		origScan.vectorStrategy = vectorStrategyFilterIndex
		// The result is every match, so short of k it is the true answer and needs no warning. But
		// with access control, whether this point is reached rather than the one below depends on
		// documents the caller cannot see, so, as below, a capped query warns whenever it is short.
		// The probe read only matches the caller can see, which is what they get back.
		if maxCandidates > 0 && n.hidesDocuments() && probe.read < k {
			n.warnCandidateLimitReached(sim, maxCandidates)
		}
		return nil
	}

	if maxCandidates > 0 {
		// The caller chose to cap the work, so return what passed. Short because the graph ran out is
		// the true answer among documents with a vector (the same ones an unfiltered search answers
		// from) and needs no warning. But on a collection with access control, the graph running out
		// would tell the caller how many documents exist, visible or not, so there it is reported
		// whenever the result is short, which the caller can see anyway.
		n.useVectorPrefixes(origScan, vectorStrategyOverFetch, prefixesOf(found.passing), found.searches)
		if !found.exhausted || n.hidesDocuments() {
			n.warnCandidateLimitReached(sim, maxCandidates)
		}
		return nil
	}

	// Short of k without a cap: the answer needs every matching document the graph did not reach, and
	// documents without a vector (which the graph never holds) too. Only reading them all gives it.
	if filterIndex.HasValue() {
		// Every match is scored exactly, as above, without reading the whole collection.
		origScan.vectorStrategy = vectorStrategyFilterIndex
		return nil
	}
	// On a collection with access control, whether this point is reached depends on documents the
	// caller cannot see, so the warning would tell them about those. The results are correct either
	// way, so it is left out there.
	if !n.hidesDocuments() {
		n.warnVectorIndexUnused(sim, reasonFilterTooSelective)
	}
	return nil
}

// useVectorPrefixes narrows the scan to the given documents found through the vector index.
func (n *selectNode) useVectorPrefixes(origScan *scanNode, strategy string, prefixes []keys.Walkable, searches int) {
	origScan.Prefixes(prefixes)
	origScan.vectorStrategy = strategy
	origScan.vectorSearches = searches
	// Empty prefixes would otherwise let the scan fall back to reading the whole collection.
	if len(prefixes) == 0 {
		origScan.noResults = true
	}
}

// hidesDocuments reports whether document access control can hide some of the collection's documents
// from the caller. Hidden documents are dropped by the scan like ones failing a filter, so the graph's
// k nearest are not enough, and whatever is decided from them must not be reported to the caller.
func (n *selectNode) hidesDocuments() bool {
	return n.planner.documentACP.HasValue() && n.collection.Version().Policy.HasValue()
}

// canCheckFilterAtScan reports whether the scan's filter decides alone which documents are in the
// result, so a document passing it here passes it in the query.
//
// That holds when the filter names only the collection's own fields. A related document's fields are
// checked after the join, an alias only once it is computed, and with lens migrations the filter is
// applied again after the documents are transformed (n.filter is then kept). Checked at the scan, any
// of those could pass documents the query later drops, and the result would come back short.
func (n *selectNode) canCheckFilterAtScan(origScan *scanNode) bool {
	if n.filter != nil {
		return false
	}
	return filterNamesOnlyOwnFields(n.collection.Version(), origScan.filter.ExternalConditions)
}

// filterNamesOnlyOwnFields reports whether every condition in the filter is on a field the collection
// stores itself, combined only with `_and`, `_or` and `_not`.
func filterNamesOnlyOwnFields(col client.CollectionVersion, conditions map[string]any) bool {
	// A filter built internally can carry only its mapped conditions. Without the named ones there is
	// no way to tell what it names.
	if conditions == nil {
		return false
	}
	for key, value := range conditions {
		switch key {
		case request.FilterOpAnd, request.FilterOpOr:
			branches, ok := value.([]any)
			if !ok {
				return false
			}
			for _, branch := range branches {
				branchConditions, ok := branch.(map[string]any)
				if !ok || !filterNamesOnlyOwnFields(col, branchConditions) {
					return false
				}
			}
		case request.FilterOpNot:
			notConditions, ok := value.(map[string]any)
			if !ok || !filterNamesOnlyOwnFields(col, notConditions) {
				return false
			}
		case request.DocIDFieldName:
		default:
			field, ok := col.GetFieldByName(key)
			if !ok || field.Kind.IsObject() {
				return false
			}
		}
	}
	return true
}

// filterIndex returns the secondary index that can resolve the scan's filter, if there is one. It is
// the index selectIndex will give the scan, so the documents read through it are the ones the scan
// will read.
func (n *selectNode) filterIndex(origScan *scanNode) immutable.Option[client.IndexDescription] {
	if origScan.filter == nil {
		return immutable.None[client.IndexDescription]()
	}
	idx := findIndexByFilter(n.collection, origScan.filter.ExternalConditions)
	// A filter on the vector field itself would resolve to the vector index, which cannot serve it.
	if !idx.HasValue() || idx.Value().IsVector() {
		return immutable.None[client.IndexDescription]()
	}
	// Some filters (an `_or` across fields) make the scan read the whole collection even with the
	// index, so reading its matches through it would be a full scan too.
	if !fetcher.CanIndexServeFilter(origScan.filter, idx.Value(), origScan.documentMapping) {
		return immutable.None[client.IndexDescription]()
	}
	return idx
}

// filterIndexProbe reads the documents a filter matches through its secondary index a few at a time,
// so the over-fetch search can find out along the way whether scoring every match would be cheaper.
// Only documents the caller can see are read.
type filterIndexProbe struct {
	origScan *scanNode
	scan     *scanNode
	// read is how many matches have been read.
	read int
	// done is set once every match has been read.
	done bool
}

// startFilterIndexProbe opens a probe over the matches of origScan's filter in the given index.
func (n *selectNode) startFilterIndexProbe(
	origScan *scanNode,
	target string,
	index client.IndexDescription,
) (*filterIndexProbe, error) {
	scan := n.checkingScan(origScan, target, immutable.Some(index))
	if err := scan.Init(); err != nil {
		return nil, errors.Join(err, scan.Close())
	}
	return &filterIndexProbe{origScan: origScan, scan: scan}, nil
}

// advance reads up to count more matches, setting done when there are none left.
func (p *filterIndexProbe) advance(count int) error {
	for range count {
		hasNext, err := p.scan.Next()
		if err != nil {
			return err
		}
		if !hasNext {
			p.done = true
			return nil
		}
		p.read++
	}
	return nil
}

// close releases the probe, counting what it read in the scan's explain output.
func (p *filterIndexProbe) close() error {
	p.origScan.execInfo.fetches.Add(p.scan.execInfo.fetches)
	return p.scan.Close()
}

// overFetchResult is what the over-fetch search found.
type overFetchResult struct {
	// passing holds the documents that passed the scan's checks, nearest first.
	passing []vectorDoc
	// searches is how many times the graph was searched.
	searches int
	// exhausted is set when the graph ran out of documents before the search stopped.
	exhausted bool
}

// overFetch searches the graph in growing batches, checking each new document the way the scan
// will, until k pass, the graph runs out, or budget documents have been examined. When probe is
// given, it is advanced by as many matches as each round examined candidates, and the search stops
// once the probe has read them all.
//
// Each batch holds the batch-size nearest documents, so once k of them pass, no document outside it
// can be nearer than those k. The answer is then the one reading the whole collection gives, as far as
// the graph search is exact: like an unfiltered search, it finds the nearest documents approximately,
// and less reliably toward the end of a large batch, which is where a selective filter's matches are.
// How approximate is set by the index's efSearch.
func (n *selectNode) overFetch(
	origScan *scanNode,
	target string,
	index client.IndexDescription,
	query []float32,
	k int,
	budget int,
	probe *filterIndexProbe,
) (overFetchResult, error) {
	var found overFetchResult
	seen := map[string]struct{}{}
	batch := min(max(overFetchFirstBatchFactor*k, 1), budget)
	for {
		results, exhausted, err := n.vectorSearch(index, query, batch)
		if err != nil {
			return overFetchResult{}, err
		}
		found.searches++

		// The graph is searched afresh each round, so only documents not yet checked are checked.
		var unchecked []vectorindex.SearchResult
		for _, r := range results {
			if _, ok := seen[r.DocID]; !ok {
				seen[r.DocID] = struct{}{}
				unchecked = append(unchecked, r)
			}
		}
		passing, err := n.passingDocs(origScan, target, unchecked)
		if err != nil {
			return overFetchResult{}, err
		}
		found.passing = append(found.passing, passing...)

		if len(found.passing) >= k {
			return found, nil
		}
		if probe != nil {
			if err := probe.advance(len(results)); err != nil {
				return overFetchResult{}, err
			}
			if probe.done {
				return found, nil
			}
		}
		if exhausted {
			found.exhausted = true
			return found, nil
		}
		if batch >= budget {
			return found, nil
		}
		batch = min(batch*overFetchGrowthFactor, budget)
	}
}

// passingDocs returns those of the given documents that pass the scan's filter and access checks,
// in the order given.
func (n *selectNode) passingDocs(
	origScan *scanNode,
	target string,
	results []vectorindex.SearchResult,
) ([]vectorDoc, error) {
	docs, err := n.docPrefixes(results)
	if err != nil {
		return nil, err
	}
	// An empty prefix list would make the scan read the whole collection.
	if len(docs) == 0 {
		return nil, nil
	}

	check := n.checkingScan(origScan, target, immutable.None[client.IndexDescription]())
	// A closure, so the fetches are read when it runs rather than when it is deferred.
	defer func() { origScan.execInfo.fetches.Add(check.execInfo.fetches) }()
	check.Prefixes(prefixesOf(docs))

	if err := check.Init(); err != nil {
		return nil, errors.Join(err, check.Close())
	}
	passed := map[string]struct{}{}
	for {
		hasNext, err := check.Next()
		if err != nil {
			return nil, errors.Join(err, check.Close())
		}
		if !hasNext {
			break
		}
		passed[check.currentValue.GetID()] = struct{}{}
	}
	if err := check.Close(); err != nil {
		return nil, err
	}

	// The scan returns documents in key order, so keep the graph's nearest-first order from docs.
	passing := make([]vectorDoc, 0, len(passed))
	for _, doc := range docs {
		if _, ok := passed[doc.docID]; ok {
			passing = append(passing, doc)
		}
	}
	return passing, nil
}

// checkingScan returns a scan that applies the same filter and access checks as origScan, reading
// only the target field. The fetcher adds the fields the filter needs.
func (n *selectNode) checkingScan(
	origScan *scanNode,
	target string,
	index immutable.Option[client.IndexDescription],
) *scanNode {
	check := origScan.cloneWithFilter(origScan.filter, index, nil)
	if field, ok := n.collection.Version().GetFieldByName(target); ok {
		check.fields = []client.CollectionFieldDescription{field}
	}
	return check
}

// similarityFields returns every `_similarity` field on the request, in the order they appear.
func (n *selectNode) similarityFields() []*mapper.Similarity {
	var found []*mapper.Similarity
	for _, field := range n.selectReq.Fields {
		if sim, ok := field.(*mapper.Similarity); ok {
			found = append(found, sim)
		}
	}
	return found
}

// isOrderedBySimilarityDesc requires ordering by this similarity alone, descending: descending
// because larger cosine means nearer, alone because a second sort key would need documents beyond
// the k the graph returns.
func (n *selectNode) isOrderedBySimilarityDesc(sim *mapper.Similarity) bool {
	if n.selectReq.OrderBy == nil || len(n.selectReq.OrderBy.Conditions) != 1 {
		return false
	}
	cond := n.selectReq.OrderBy.Conditions[0]
	if cond.Direction != mapper.DESC {
		return false
	}
	return len(cond.FieldIndexes) == 1 && cond.FieldIndexes[0] == sim.Field.Index
}

// readyVectorIndexOnField returns the vector index on the field, if any. queryableIndexesOnField has
// already excluded indexes that are still building or have failed, so a returned index is usable.
//
// Any metric qualifies, because similarityNode scores by the index's metric, so the k documents the
// graph returns are the k the query asked for.
func (n *selectNode) readyVectorIndexOnField(fieldName string) (client.IndexDescription, bool) {
	for _, idx := range queryableIndexesOnField(n.collection, fieldName) {
		if idx.IsVector() {
			return idx, true
		}
	}
	return client.IndexDescription{}, false
}

// vectorIndexMetricOnField returns the metric of the field's vector index, or cosine if it has no
// index. Cosine is the default because that is what `_similarity` meant before any metric existed, so
// an unindexed field keeps scoring as it did.
func vectorIndexMetricOnField(col client.Collection, fieldName string) client.DistanceMetric {
	for _, idx := range queryableIndexesOnField(col, fieldName) {
		if vector, ok := idx.GetVector(); ok {
			return vector.Metric
		}
	}
	return client.DistanceMetricCosine
}

// vectorSearch runs the graph search for the k documents nearest to query. exhausted is set when the
// graph holds fewer than k.
func (n *selectNode) vectorSearch(
	index client.IndexDescription,
	query []float32,
	k int,
) (results []vectorindex.SearchResult, exhausted bool, err error) {
	// This is only reached for a vector index (the caller already checked), so GetVector always succeeds.
	vectorDesc, _ := index.GetVector()

	collectionShortID, err := id.GetCollectionShortID(n.planner.ctx, n.collection.Version().CollectionID)
	if err != nil {
		return nil, false, err
	}

	epoch, err := fetcher.ReadIndexEpoch(
		n.planner.ctx,
		datastore.CtxMustGetTxn(n.planner.ctx),
		n.collection.Version().CollectionID,
		index.ID,
	)
	if err != nil {
		return nil, false, err
	}

	return vectorindex.Search(n.planner.ctx, collectionShortID, index.ID, epoch, *vectorDesc, query, k)
}

// vectorDoc is a document found by the graph search, with the prefix the scan reads it by.
type vectorDoc struct {
	docID  string
	prefix keys.Walkable
	// distance is the document's distance to the query under the index's metric (smaller is nearer).
	distance float64
}

// docPrefixes returns the prefix of each search result, in the same order. A result whose document no
// longer has a short id is left out.
func (n *selectNode) docPrefixes(results []vectorindex.SearchResult) ([]vectorDoc, error) {
	collectionShortID, err := id.GetCollectionShortID(n.planner.ctx, n.collection.Version().CollectionID)
	if err != nil {
		return nil, err
	}

	docs := make([]vectorDoc, 0, len(results))
	for _, r := range results {
		docShortID, found, err := id.GetDocShortID(n.planner.ctx, collectionShortID, r.DocID)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		docs = append(docs, vectorDoc{
			docID: r.DocID,
			prefix: keys.DataStoreKey{
				CollectionShortID: collectionShortID,
				DocShortID:        docShortID,
			},
			distance: r.Distance,
		})
	}
	return docs, nil
}

// nearest returns the k nearest of docs. The last round can pass up to twice as many as the result
// needs, and only the nearest k can be in it, so the scan need not read the others again. Distances
// come from each document's stored vector under the index's metric, so they rank the documents the
// same way the query's scores do.
func nearest(docs []vectorDoc, k int) []vectorDoc {
	if len(docs) <= k {
		return docs
	}
	sorted := slices.Clone(docs)
	slices.SortStableFunc(sorted, func(a, b vectorDoc) int {
		return cmp.Compare(a.distance, b.distance)
	})
	return sorted[:k]
}

// prefixesOf returns the prefixes of docs, in the same order.
func prefixesOf(docs []vectorDoc) []keys.Walkable {
	prefixes := make([]keys.Walkable, len(docs))
	for i, doc := range docs {
		prefixes[i] = doc.prefix
	}
	return prefixes
}

func similarityQueryVector(vector any) ([]float32, bool) {
	vec := convertArray[float32](vector)
	if vec == nil {
		return nil, false
	}
	return vec, true
}
