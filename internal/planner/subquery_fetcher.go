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
	"context"
	"errors"
	"maps"

	"github.com/sourcenetwork/immutable"

	lensStore "github.com/sourcenetwork/lens/host-go/store"

	"github.com/sourcenetwork/defradb/acp/dac"
	acpIdentity "github.com/sourcenetwork/defradb/acp/identity"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/internal/connor"
	"github.com/sourcenetwork/defradb/internal/core"
	"github.com/sourcenetwork/defradb/internal/datastore"
	acpDB "github.com/sourcenetwork/defradb/internal/db/acp"
	"github.com/sourcenetwork/defradb/internal/db/fetcher"
	"github.com/sourcenetwork/defradb/internal/db/id"
	"github.com/sourcenetwork/defradb/internal/keys"
	"github.com/sourcenetwork/defradb/internal/lens"
	"github.com/sourcenetwork/defradb/internal/planner/filter"
	"github.com/sourcenetwork/defradb/internal/planner/mapper"
)

// subQueryFetcher executes side-channel fetches independently of the main plan tree.
//
// It was introduced for orphan detection, which needs to fetch documents outside the
// normal plan iteration (e.g., "fetch all parents where FK IS NULL" or "fetch all
// parents except these IDs"). These fetches cannot go through the existing scanNode
// because it is already in use by the join's main iteration — mutating its filter or
// fetcher mid-iteration would corrupt state. subQueryFetcher avoids this by creating
// its own fetcher instance for each query, with proper cleanup via defer Close().
//
// Two usage patterns:
//
// Batch: fetchDocs / fetchOrphans — run a full pipeline and return all docs.
//
// Iterator: initIterator → nextDoc (repeated) → closeIterator — lazy one-at-a-time iteration.
type subQueryFetcher struct {
	ctx         context.Context
	identity    immutable.Option[acpIdentity.Identity]
	nodeACP     acpDB.NACInfo
	documentACP immutable.Option[dac.DocumentACP]
	col         client.Collection
	docMapping  *core.DocumentMapping
	lensStore   lensStore.Store

	// fields to fetch for each document
	fields []client.CollectionFieldDescription

	// execInfo accumulates fetch stats across all fetches
	execInfo *fetcher.ExecInfo

	// Iterator state — set by initIterator, consumed by nextDoc, cleaned up by closeIterator.
	iterFetcher fetcher.Fetcher
	iterShortID uint32
}

// newSubQueryFetcher creates a fetcher for sub-query execution.
// The execInfo parameter allows stats to be accumulated across multiple fetches.
func newSubQueryFetcher(
	ctx context.Context,
	identity immutable.Option[acpIdentity.Identity],
	nodeACP acpDB.NACInfo,
	documentACP immutable.Option[dac.DocumentACP],
	col client.Collection,
	docMapping *core.DocumentMapping,
	lensStore lensStore.Store,
	fields []client.CollectionFieldDescription,
	execInfo *fetcher.ExecInfo,
) *subQueryFetcher {
	return &subQueryFetcher{
		ctx:         ctx,
		identity:    identity,
		nodeACP:     nodeACP,
		documentACP: documentACP,
		col:         col,
		docMapping:  docMapping,
		lensStore:   lensStore,
		fields:      fields,
		execInfo:    execInfo,
	}
}

// createFetcher creates a new fetcher instance wrapped with lens support.
func (f *subQueryFetcher) createFetcher() fetcher.Fetcher {
	baseFetcher := fetcher.NewDocumentFetcher()
	return lens.NewFetcher(baseFetcher, f.lensStore)
}

// fetchDocs runs a full fetch pipeline: selectIndex → Init → Start → collect.
// The filter and relationIDFieldName control index selection.
func (f *subQueryFetcher) fetchDocs(
	filter *mapper.Filter,
	relationIDFieldName string,
) (docs []core.Doc, err error) {
	txn := datastore.CtxMustGetTxn(f.ctx)

	shortID, err := id.GetShortCollectionID(f.ctx, f.col.Version().CollectionID)
	if err != nil {
		return nil, err
	}

	result := selectIndex(selectIndexOptions{
		collection:          f.col,
		filter:              filter,
		relationIDFieldName: relationIDFieldName,
		docMapping:          f.docMapping,
	})

	fetch := f.createFetcher()
	defer func() {
		err = errors.Join(err, fetch.Close())
	}()

	err = fetch.Init(
		f.ctx,
		f.identity,
		txn,
		f.nodeACP,
		f.documentACP,
		result.index,
		f.col,
		f.fields,
		filter,
		nil,
		f.docMapping,
		false,
	)
	if err != nil {
		return nil, err
	}

	prefix := keys.DataStoreKey{CollectionShortID: shortID}
	err = fetch.Start(f.ctx, prefix)
	if err != nil {
		return nil, err
	}

	return f.collectDocs(fetch, shortID)
}

// fetchOrphans fetches documents where the relation ID field is NULL.
func (f *subQueryFetcher) fetchOrphans(
	relIDFieldName string,
	relIDFieldMapIndex int,
	filter *mapper.Filter,
) ([]core.Doc, error) {
	filterWithNull := addNullFilterOnField(filter, relIDFieldMapIndex)
	return f.fetchDocs(filterWithNull, relIDFieldName)
}

// hasDoc checks whether at least one document exists matching the given filter
// and relation ID field. It performs a single FetchNext and returns true if a
// document was found.
func (f *subQueryFetcher) hasDoc(
	filter *mapper.Filter,
	relationIDFieldName string,
) (bool, error) {
	txn := datastore.CtxMustGetTxn(f.ctx)

	shortID, err := id.GetShortCollectionID(f.ctx, f.col.Version().CollectionID)
	if err != nil {
		return false, err
	}

	result := selectIndex(selectIndexOptions{
		collection:          f.col,
		filter:              filter,
		relationIDFieldName: relationIDFieldName,
		docMapping:          f.docMapping,
	})

	fetch := f.createFetcher()
	defer func() {
		err = errors.Join(err, fetch.Close())
	}()

	err = fetch.Init(
		f.ctx,
		f.identity,
		txn,
		f.nodeACP,
		f.documentACP,
		result.index,
		f.col,
		f.fields,
		filter,
		nil,
		f.docMapping,
		false,
	)
	if err != nil {
		return false, err
	}

	prefix := keys.DataStoreKey{CollectionShortID: shortID}
	err = fetch.Start(f.ctx, prefix)
	if err != nil {
		return false, err
	}

	encDoc, fetchExecInfo, err := fetch.FetchNext(f.ctx)
	if err != nil {
		return false, err
	}

	if f.execInfo != nil {
		f.execInfo.Add(fetchExecInfo)
	}

	return encDoc != nil, nil
}

// initIterator sets up the fetch pipeline for lazy iteration via nextDoc.
// Must be followed by nextDoc calls and a closeIterator when done.
func (f *subQueryFetcher) initIterator(
	filter *mapper.Filter,
	relationIDFieldName string,
) error {
	txn := datastore.CtxMustGetTxn(f.ctx)

	shortID, err := id.GetShortCollectionID(f.ctx, f.col.Version().CollectionID)
	if err != nil {
		return err
	}

	result := selectIndex(selectIndexOptions{
		collection:          f.col,
		filter:              filter,
		relationIDFieldName: relationIDFieldName,
		docMapping:          f.docMapping,
	})

	fetch := f.createFetcher()

	err = fetch.Init(
		f.ctx,
		f.identity,
		txn,
		f.nodeACP,
		f.documentACP,
		result.index,
		f.col,
		f.fields,
		filter,
		nil,
		f.docMapping,
		false,
	)
	if err != nil {
		_ = fetch.Close()
		return err
	}

	prefix := keys.DataStoreKey{CollectionShortID: shortID}
	err = fetch.Start(f.ctx, prefix)
	if err != nil {
		_ = fetch.Close()
		return err
	}

	f.iterFetcher = fetch
	f.iterShortID = shortID
	return nil
}

// nextDoc returns the next document from the iterator, or false when exhausted.
// Must be called after initIterator.
func (f *subQueryFetcher) nextDoc() (core.Doc, bool, error) {
	if f.iterFetcher == nil {
		return core.Doc{}, false, nil
	}

	encDoc, fetchExecInfo, err := f.iterFetcher.FetchNext(f.ctx)
	if err != nil {
		return core.Doc{}, false, err
	}

	if f.execInfo != nil {
		f.execInfo.Add(fetchExecInfo)
	}

	if encDoc == nil {
		return core.Doc{}, false, nil
	}

	doc, err := fetcher.DecodeToDoc(f.ctx, f.iterShortID, encDoc, f.docMapping, false)
	if err != nil {
		return core.Doc{}, false, err
	}

	return doc, true, nil
}

// closeIterator releases resources held by the iterator.
func (f *subQueryFetcher) closeIterator() error {
	if f.iterFetcher == nil {
		return nil
	}
	err := f.iterFetcher.Close()
	f.iterFetcher = nil
	return err
}

// addFilterOnField returns a new filter with a condition that checks if the field equals the given value.
// It does not mutate the input filter.
func addFilterOnField(f *mapper.Filter, propIndex int, value any) *mapper.Filter {
	result := mapper.NewFilter()
	if f != nil {
		maps.Copy(result.Conditions, f.Conditions)
		result.ExternalConditions = make(map[string]any, len(f.ExternalConditions))
		maps.Copy(result.ExternalConditions, f.ExternalConditions)
	}

	propertyIndex := &mapper.PropertyIndex{Index: propIndex}
	filterConditions := map[connor.FilterKey]any{
		propertyIndex: map[connor.FilterKey]any{
			mapper.FilterEqOp: value,
		},
	}

	filter.RemoveField(result, mapper.Field{Index: propIndex})
	result.Conditions = filter.MergeConditions(result.Conditions, filterConditions)
	return result
}

// addNullFilterOnField adds a filter condition that checks if the field is NULL.
func addNullFilterOnField(f *mapper.Filter, propIndex int) *mapper.Filter {
	return addFilterOnField(f, propIndex, nil)
}

// collectDocs fetches all documents from the fetcher.
func (f *subQueryFetcher) collectDocs(fetch fetcher.Fetcher, shortID uint32) ([]core.Doc, error) {
	var docs []core.Doc

	for {
		encDoc, fetchExecInfo, err := fetch.FetchNext(f.ctx)
		if err != nil {
			return nil, err
		}

		if f.execInfo != nil {
			f.execInfo.Add(fetchExecInfo)
		}

		if encDoc == nil {
			break
		}

		doc, err := fetcher.DecodeToDoc(f.ctx, shortID, encDoc, f.docMapping, false)
		if err != nil {
			return nil, err
		}

		docs = append(docs, doc)
	}

	return docs, nil
}
