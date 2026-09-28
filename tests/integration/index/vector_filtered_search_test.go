// Copyright 2026 Democratized Data Foundation
//
// This file is part of the DefraDB test suite.
//
// The DefraDB test suite is licensed under either:
//
//   (1) GNU Affero General Public License v3
//   (2) Business Source License 1.1
//
// See tests/LICENSE for details.

package index

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/tests/action"
	testUtils "github.com/sourcenetwork/defradb/tests/integration"
)

// These tests check filtered nearest-neighbour queries against a brute-force baseline: the test
// scores every document itself, applies the filter itself, and expects exactly the nearest matches.
// Using the vector index must never change that answer, only how fast it arrives, so each case also
// asserts through explain which strategy answered it. Otherwise two cases meant for different
// strategies could both be taking the same one and nobody would know.

// filteredSearchLimit is k: the limit every case below asks for.
const filteredSearchLimit = 5

// filteredSearchQuery is the vector every case searches for.
var filteredSearchQuery = []float64{0.3, -0.7, 0.5, 0.1}

var filteredSearchMetrics = []client.DistanceMetric{
	client.DistanceMetricCosine,
	client.DistanceMetricEuclidean,
	client.DistanceMetricDotProduct,
}

// filteredSearchDoc is one document of the baseline data set.
type filteredSearchDoc struct {
	name string
	// n is unique per document and not indexed, so a filter on it is checked against candidates the
	// vector index returns (the over-fetch strategy).
	n int
	// bucket is n modulo 20 and has a secondary index, so a filter on it is resolved through that
	// index (the filter-index strategy).
	bucket int
	vector []float64
}

// filteredSearchDocs returns count documents with pseudo-random vectors. The seed is fixed so every run
// and every client sees the same data.
func filteredSearchDocs(count int) []filteredSearchDoc {
	r := rand.New(rand.NewPCG(5071, 1))
	docs := make([]filteredSearchDoc, count)
	for i := range docs {
		vector := make([]float64, len(filteredSearchQuery))
		for j := range vector {
			// Two decimals keep the request readable. The value is then taken through float32, as the
			// field stores it, so the baseline scores the same numbers the database does.
			vector[j] = float64(float32(math.Round((r.Float64()*2-1)*100) / 100))
		}
		docs[i] = filteredSearchDoc{
			name:   fmt.Sprintf("d%04d", i),
			n:      i,
			bucket: i % 20,
			vector: vector,
		}
	}
	return docs
}

// filteredSearchSetup creates the collection with a vector index of the given metric and adds docs.
//
// Each document is its own action rather than one mutation adding them all: the test waits for every
// document's update event, and one mutation adding more than the event buffer holds (100) would leave
// them unread and block the node from closing.
func filteredSearchSetup(metric client.DistanceMetric, docs []filteredSearchDoc) []any {
	actions := []any{
		&action.AddCollection{
			SDL: `type User {
				name: String
				n: Int
				bucket: Int @index
				vector: [Float32!] @index(vector: {dimensions: 4, hnsw: {metric: ` + string(metric) + `}})
			}`,
		},
	}
	for _, doc := range docs {
		vector := make([]float32, len(doc.vector))
		for i, v := range doc.vector {
			vector[i] = float32(v)
		}
		actions = append(actions, &action.AddDoc{DocMap: map[string]any{
			"name":   doc.name,
			"n":      doc.n,
			"bucket": doc.bucket,
			"vector": vector,
		}})
	}
	return actions
}

// nearestMatches is the brute-force baseline: the documents passing the filter, nearest first, from
// offset up to limit of them. Only candidates are considered, which is every document unless a case
// says otherwise.
func nearestMatches(
	t *testing.T,
	metric client.DistanceMetric,
	candidates []filteredSearchDoc,
	passes func(filteredSearchDoc) bool,
	offset, limit int,
) []map[string]any {
	var matching []filteredSearchDoc
	for _, doc := range candidates {
		if passes(doc) {
			matching = append(matching, doc)
		}
	}
	scores := make(map[string]float64, len(matching))
	for _, doc := range matching {
		scores[doc.name] = testUtils.ExpectedSimilarity(metric, doc.vector, filteredSearchQuery)
	}
	slices.SortFunc(matching, func(a, b filteredSearchDoc) int {
		return -compareFloats(scores[a.name], scores[b.name])
	})

	// Two documents scoring (almost) the same could come back in either order, or trade places at the
	// limit, so a case built on them would pass or fail by accident. The data is fixed, so this only
	// guards a change to it.
	end := min(offset+limit, len(matching))
	for i := 1; i < min(end+1, len(matching)); i++ {
		gap := scores[matching[i-1].name] - scores[matching[i].name]
		require.Greater(t, gap, 1e-6, "documents %s and %s score too close to rank reliably",
			matching[i-1].name, matching[i].name)
	}

	results := []map[string]any{}
	for _, doc := range matching[min(offset, len(matching)):end] {
		results = append(results, map[string]any{
			"name": doc.name,
			"sim":  testUtils.SimilarityScore(metric, doc.vector, filteredSearchQuery),
		})
	}
	return results
}

func compareFloats(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// nearestDocs returns the count documents nearest the query, regardless of any filter: what the vector
// index returns when asked for count.
func nearestDocs(metric client.DistanceMetric, docs []filteredSearchDoc, count int) []filteredSearchDoc {
	sorted := slices.Clone(docs)
	slices.SortFunc(sorted, func(a, b filteredSearchDoc) int {
		return -compareFloats(
			testUtils.ExpectedSimilarity(metric, a.vector, filteredSearchQuery),
			testUtils.ExpectedSimilarity(metric, b.vector, filteredSearchQuery),
		)
	})
	return sorted[:min(count, len(sorted))]
}

// filteredSearchRequest builds the nearest-neighbour query. similarityArgs are extra arguments to the
// SIMILARITY selector, such as maxCandidates.
func filteredSearchRequest(filter string, limit, offset int, similarityArgs string) string {
	values := make([]string, len(filteredSearchQuery))
	for i, v := range filteredSearchQuery {
		values[i] = strconv.FormatFloat(v, 'g', -1, 64)
	}
	return fmt.Sprintf(`query {
		User(filter: %s, order: {_alias: {sim: DESC}}, limit: %d, offset: %d) {
			name
			sim: SIMILARITY(vector: {vector: [%s]%s})
		}
	}`, filter, limit, offset, strings.Join(values, ", "), similarityArgs)
}

// filteredSearchCase is one query run on every metric.
type filteredSearchCase struct {
	name   string
	filter string
	passes func(filteredSearchDoc) bool
	offset int
	// strategy is the vectorStrategy explain must report; empty asserts none ran (a full scan).
	strategy string
	// warnings are the warnings the query must carry; none asserts it carries none.
	warnings []client.GQLWarning
}

func TestVectorIndex_FilteredSearch_MatchesBruteForceAcrossSelectivities(t *testing.T) {
	const docCount = 200
	docs := filteredSearchDocs(docCount)

	cases := []filteredSearchCase{
		{
			name:     "filter passes almost every document",
			filter:   `{n: {_geq: 4}}`,
			passes:   func(d filteredSearchDoc) bool { return d.n >= 4 },
			strategy: "overFetch",
		},
		{
			name:     "filter passes about half",
			filter:   `{n: {_lt: 100}}`,
			passes:   func(d filteredSearchDoc) bool { return d.n < 100 },
			strategy: "overFetch",
		},
		{
			name:     "filter passes exactly the limit",
			filter:   `{n: {_lt: 5}}`,
			passes:   func(d filteredSearchDoc) bool { return d.n < 5 },
			strategy: "overFetch",
		},
		{
			name:     "offset is filled from past the limit",
			filter:   `{n: {_lt: 100}}`,
			passes:   func(d filteredSearchDoc) bool { return d.n < 100 },
			offset:   3,
			strategy: "overFetch",
		},
		{
			name:   "filter passes fewer than the limit",
			filter: `{n: {_lt: 3}}`,
			passes: func(d filteredSearchDoc) bool { return d.n < 3 },
			// No secondary index can find the rest, so the whole collection is read to be sure there
			// is no other match, and the query says so.
			warnings: []client.GQLWarning{{
				Code: client.WarningCodeVectorIndexUnused,
				Detail: map[string]any{
					"field":  "vector",
					"reason": "filterTooSelective",
				},
			}},
		},
		{
			name:     "filter resolved by a secondary index",
			filter:   `{bucket: {_eq: 3}}`,
			passes:   func(d filteredSearchDoc) bool { return d.bucket == 3 },
			strategy: "filterIndex",
		},
		{
			// The secondary index cannot serve an `_or` across fields: the scan would read the whole
			// collection with it anyway, so it is not the one to count or answer with.
			name:     "filter on an indexed and an unindexed field joined by or",
			filter:   `{_or: [{bucket: {_eq: 3}}, {n: {_lt: 100}}]}`,
			passes:   func(d filteredSearchDoc) bool { return d.bucket == 3 || d.n < 100 },
			strategy: "overFetch",
		},
		{
			name:     "filter resolved by a secondary index passes fewer than the limit",
			filter:   `{bucket: {_eq: 3}, n: {_lt: 40}}`,
			passes:   func(d filteredSearchDoc) bool { return d.bucket == 3 && d.n < 40 },
			strategy: "filterIndex",
		},
	}

	// The collection is set up once per metric and every case runs against it: setting it up is by far
	// the slowest part.
	for _, metric := range filteredSearchMetrics {
		t.Run(string(metric), func(t *testing.T) {
			actions := filteredSearchSetup(metric, docs)
			for _, c := range cases {
				req := filteredSearchRequest(c.filter, filteredSearchLimit, c.offset, "")
				actions = append(actions,
					&action.Request{
						Request: req,
						Results: map[string]any{
							"User": nearestMatches(t, metric, docs, c.passes, c.offset, filteredSearchLimit),
						},
						ExpectedWarnings: c.warnings,
					},
					&action.Request{
						Request:          makeExplainQuery(req),
						Asserter:         testUtils.NewExplainAsserter().WithVectorStrategy(c.strategy),
						ExpectedWarnings: c.warnings,
					},
				)
			}
			testUtils.ExecuteTestCase(t, testUtils.TestCase{Actions: actions})
		})
	}
}

// The expected result of a capped search is not the brute-force answer: it is the documents passing
// the filter among the maxCandidates nearest. A test asserting only the warning would pass even if the
// wrong documents came back, so the documents are checked too.
func TestVectorIndex_FilteredSearchWithMaxCandidates_ReturnsMatchesAmongNearestAndWarns(t *testing.T) {
	const docCount = 200
	docs := filteredSearchDocs(docCount)
	passes := func(d filteredSearchDoc) bool { return d.n < 100 }

	for _, metric := range filteredSearchMetrics {
		t.Run(string(metric), func(t *testing.T) {
			examined := nearestDocs(metric, docs, filteredSearchLimit)
			expected := nearestMatches(t, metric, examined, passes, 0, filteredSearchLimit)
			// If every one of the nearest passed, the result would be full and there would be nothing
			// to warn about, so the case would test nothing.
			require.Less(t, len(expected), filteredSearchLimit, "the capped search must come back short")

			req := filteredSearchRequest(`{n: {_lt: 100}}`, filteredSearchLimit, 0, ", maxCandidates: 5")
			warnings := []client.GQLWarning{{
				Code: client.WarningCodeVectorCandidateLimitReached,
				Detail: map[string]any{
					"field":         "vector",
					"limit":         filteredSearchLimit,
					"maxCandidates": 5,
				},
			}}
			test := testUtils.TestCase{
				Actions: append(filteredSearchSetup(metric, docs),
					&action.Request{
						Request:          req,
						Results:          map[string]any{"User": expected},
						ExpectedWarnings: warnings,
					},
					&action.Request{
						Request:          makeExplainQuery(req),
						Asserter:         testUtils.NewExplainAsserter().WithVectorStrategy("overFetch"),
						ExpectedWarnings: warnings,
					},
				),
			}
			testUtils.ExecuteTestCase(t, test)
		})
	}
}

// A result that is short because only that many documents match is the true answer, so it must not
// carry the warning. Otherwise the warning would fire on every selective query and mean nothing.
func TestVectorIndex_FilteredSearchWithMaxCandidates_FewerMatchesThanLimit_ReportsNoWarning(t *testing.T) {
	const docCount = 200
	docs := filteredSearchDocs(docCount)
	passes := func(d filteredSearchDoc) bool { return d.n < 3 }

	for _, metric := range filteredSearchMetrics {
		t.Run(string(metric), func(t *testing.T) {
			expected := nearestMatches(t, metric, docs, passes, 0, filteredSearchLimit)
			require.Len(t, expected, 3)

			// The cap is larger than the collection, so the index runs out of documents first: every
			// match was seen.
			req := filteredSearchRequest(`{n: {_lt: 3}}`, filteredSearchLimit, 0, ", maxCandidates: 1000")
			test := testUtils.TestCase{
				Actions: append(filteredSearchSetup(metric, docs),
					&action.Request{
						Request: req,
						Results: map[string]any{"User": expected},
					},
					&action.Request{
						Request:  makeExplainQuery(req),
						Asserter: testUtils.NewExplainAsserter().WithVectorStrategy("overFetch"),
					},
				),
			}
			testUtils.ExecuteTestCase(t, test)
		})
	}
}

// A cap large enough to fill the limit changes nothing: the full answer and no warning.
func TestVectorIndex_FilteredSearchWithMaxCandidates_LimitFilled_ReportsNoWarning(t *testing.T) {
	docs := filteredSearchDocs(200)
	metric := client.DistanceMetricCosine
	passes := func(d filteredSearchDoc) bool { return d.n < 100 }

	test := testUtils.TestCase{
		Actions: append(filteredSearchSetup(metric, docs),
			&action.Request{
				Request: filteredSearchRequest(`{n: {_lt: 100}}`, filteredSearchLimit, 0, ", maxCandidates: 50"),
				Results: map[string]any{
					"User": nearestMatches(t, metric, docs, passes, 0, filteredSearchLimit),
				},
			},
		),
	}
	testUtils.ExecuteTestCase(t, test)
}

// Fewer candidates than the limit could never fill it, so the cap is raised to the limit, and the
// warning reports the cap that was applied.
func TestVectorIndex_FilteredSearchWithMaxCandidatesBelowLimit_RaisesItToTheLimit(t *testing.T) {
	docs := filteredSearchDocs(200)
	metric := client.DistanceMetricCosine
	passes := func(d filteredSearchDoc) bool { return d.n < 100 }
	examined := nearestDocs(metric, docs, filteredSearchLimit)

	test := testUtils.TestCase{
		Actions: append(filteredSearchSetup(metric, docs),
			&action.Request{
				Request: filteredSearchRequest(`{n: {_lt: 100}}`, filteredSearchLimit, 0, ", maxCandidates: 1"),
				Results: map[string]any{
					"User": nearestMatches(t, metric, examined, passes, 0, filteredSearchLimit),
				},
				ExpectedWarnings: []client.GQLWarning{{
					Code: client.WarningCodeVectorCandidateLimitReached,
					Detail: map[string]any{
						"field":         "vector",
						"limit":         filteredSearchLimit,
						"maxCandidates": filteredSearchLimit,
					},
				}},
			},
		),
	}
	testUtils.ExecuteTestCase(t, test)
}

func TestVectorIndex_FilteredSearchWithZeroMaxCandidates_Errors(t *testing.T) {
	test := testUtils.TestCase{
		Actions: append(filteredSearchSetup(client.DistanceMetricCosine, filteredSearchDocs(10)),
			&action.Request{
				Request:       filteredSearchRequest(`{n: {_lt: 5}}`, filteredSearchLimit, 0, ", maxCandidates: 0"),
				ExpectedError: "similarity maxCandidates must be at least 1",
			},
		),
	}
	testUtils.ExecuteTestCase(t, test)
}

// A filter with a secondary index that most documents pass is answered by the vector index: a few of
// the nearest documents fill the limit long before the secondary index could list every match. The
// cost is the proof: had the scan read every match through the secondary index, or counted them all
// before choosing, it would be about 190 fetches, not a couple of dozen.
func TestVectorIndex_FilteredSearch_NonSelectiveIndexedFilter_SearchesVectorIndex(t *testing.T) {
	docs := filteredSearchDocs(200)
	metric := client.DistanceMetricCosine
	passes := func(d filteredSearchDoc) bool { return d.bucket < 19 }
	req := filteredSearchRequest(`{bucket: {_lt: 19}}`, filteredSearchLimit, 0, "")

	// The first batch (twice the limit) is fetched to check the filter, enough of them pass, and the
	// scan reads only those that passed. The secondary index is opened but never read, because the
	// vector index finished first.
	firstBatch := nearestDocs(metric, docs, 2*filteredSearchLimit)
	passing := 0
	for _, doc := range firstBatch {
		if passes(doc) {
			passing++
		}
	}
	require.GreaterOrEqual(t, passing, filteredSearchLimit, "the first batch must fill the limit")

	test := testUtils.TestCase{
		Actions: append(filteredSearchSetup(metric, docs),
			&action.Request{
				Request: req,
				Results: map[string]any{
					"User": nearestMatches(t, metric, docs, passes, 0, filteredSearchLimit),
				},
			},
			&action.Request{
				Request: makeExplainQuery(req),
				Asserter: testUtils.NewExplainAsserter().
					WithVectorStrategy("overFetch").
					WithDocFetches(len(firstBatch) + passing),
			},
		),
	}
	testUtils.ExecuteTestCase(t, test)
}

// Every document near the query fails the filter, and the filter matches more documents than the
// secondary index is read for while the search runs, so the search gives up at its budget before
// either way has an answer. The secondary index then scores every match: the same answer, without
// reading the whole collection and without warning.
func TestVectorIndex_FilteredSearch_NearestAllFailFilter_FallsBackToFilterIndex(t *testing.T) {
	metric := client.DistanceMetricCosine
	// Matches (bucket 0) point away from the query and non-matches (bucket 1) toward it, so the
	// nearest documents, past the search's budget of 1024, are all ones the filter rejects. Over its
	// rounds the search reads the secondary index for about 2300 matches, so there are more than that.
	const nonMatching, matching = 1100, 2400
	var docs []filteredSearchDoc
	for i := range nonMatching + matching {
		bucket := 0
		if i < nonMatching {
			bucket = 1
		}
		doc := filteredSearchDoc{name: fmt.Sprintf("d%04d", i), n: i, bucket: bucket}
		jitter := float64(i) / 10000
		if doc.bucket == 0 {
			doc.vector = []float64{
				float64(float32(-0.3 - jitter)), float64(float32(0.7)), float64(float32(-0.5)), float64(float32(-0.1)),
			}
		} else {
			doc.vector = []float64{
				float64(float32(0.3 + jitter)), float64(float32(-0.7)), float64(float32(0.5)), float64(float32(0.1)),
			}
		}
		docs = append(docs, doc)
	}
	passes := func(d filteredSearchDoc) bool { return d.bucket == 0 }
	req := filteredSearchRequest(`{bucket: {_eq: 0}}`, filteredSearchLimit, 0, "")

	test := testUtils.TestCase{
		Actions: append(filteredSearchSetup(metric, docs),
			&action.Request{
				Request: req,
				Results: map[string]any{
					"User": nearestMatches(t, metric, docs, passes, 0, filteredSearchLimit),
				},
			},
			&action.Request{
				Request:  makeExplainQuery(req),
				Asserter: testUtils.NewExplainAsserter().WithVectorStrategy("filterIndex"),
			},
		),
	}
	testUtils.ExecuteTestCase(t, test)
}

// With a cap, the secondary index read alongside the search can still list every match before the cap
// is reached. Scoring those is exact, so the result is complete and carries no warning, even though
// the capped search alone would have come back short.
func TestVectorIndex_FilteredSearchWithMaxCandidates_SecondaryIndexListsAllMatches_ReportsNoWarning(t *testing.T) {
	docs := filteredSearchDocs(200)
	passes := func(d filteredSearchDoc) bool { return d.bucket == 3 && d.n < 40 }

	for _, metric := range filteredSearchMetrics {
		t.Run(string(metric), func(t *testing.T) {
			expected := nearestMatches(t, metric, docs, passes, 0, filteredSearchLimit)
			require.Len(t, expected, 2)

			req := filteredSearchRequest(`{bucket: {_eq: 3}, n: {_lt: 40}}`, filteredSearchLimit, 0, ", maxCandidates: 5")
			test := testUtils.TestCase{
				Actions: append(filteredSearchSetup(metric, docs),
					&action.Request{
						Request: req,
						Results: map[string]any{"User": expected},
					},
					&action.Request{
						Request:  makeExplainQuery(req),
						Asserter: testUtils.NewExplainAsserter().WithVectorStrategy("filterIndex"),
					},
				),
			}
			testUtils.ExecuteTestCase(t, test)
		})
	}
}
