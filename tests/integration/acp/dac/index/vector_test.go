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

package test_acp_dac_index

import (
	"testing"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/tests/action"
	testUtils "github.com/sourcenetwork/defradb/tests/integration"
)

// vectorACPSetup adds two documents only identity 1 can read, nearest the query [1, 0, 0], and two
// public ones farther away. The vector index holds all four, so its nearest documents are ones
// identity 2 is not allowed to see.
func vectorACPSetup() []any {
	return []any{
		testUtils.AddDACPolicy{
			Identity: testUtils.ClientIdentity(1),
			Policy:   userPolicy,
		},
		&action.AddCollection{
			SDL: `
				type Users @policy(
					id: "{{.Policy0}}",
					resource: "users"
				) {
					name: String
					vector: [Float32!] @index(vector: {dimensions: 3, hnsw: {metric: COSINE}})
				}
			`,
		},
		&action.AddDoc{
			Identity: testUtils.ClientIdentity(1),
			DocMap:   map[string]any{"name": "private1", "vector": []float32{1, 0, 0}},
		},
		&action.AddDoc{
			Identity: testUtils.ClientIdentity(1),
			DocMap:   map[string]any{"name": "private2", "vector": []float32{0.9, 0.1, 0}},
		},
		&action.AddDoc{DocMap: map[string]any{"name": "public1", "vector": []float32{0.5, 0.5, 0}}},
		&action.AddDoc{DocMap: map[string]any{"name": "public2", "vector": []float32{0, 1, 0}}},
	}
}

var vectorACPPublicResults = []map[string]any{
	{"name": "public1", "sim": testUtils.CosineSimilarity([]float64{0.5, 0.5, 0}, []float64{1, 0, 0})},
	{"name": "public2", "sim": testUtils.CosineSimilarity([]float64{0, 1, 0}, []float64{1, 0, 0})},
}

// Documents the caller cannot see are dropped like documents failing a filter. Taking only the index's
// k nearest would return nothing here, because both of them are hidden.
func TestACP_VectorIndexQuery_NearestHiddenFromCaller_ReturnsNearestVisible(t *testing.T) {
	test := testUtils.TestCase{
		Actions: append(vectorACPSetup(),
			&action.Request{
				Identity: testUtils.ClientIdentity(2),
				Request: `query {
					Users(order: {_alias: {sim: DESC}}, limit: 2) {
						name
						sim: SIMILARITY(vector: {vector: [1, 0, 0]})
					}
				}`,
				Results: map[string]any{"Users": vectorACPPublicResults},
			},
		),
	}

	testUtils.ExecuteTestCase(t, test)
}

// Fewer documents are visible than the limit, so the whole collection is read to find them all. The
// query does not say so: whether it got that far depends on the hidden documents, and saying so would
// tell the caller about them.
func TestACP_VectorIndexQuery_FewerVisibleThanLimit_ReportsNoWarning(t *testing.T) {
	test := testUtils.TestCase{
		Actions: append(vectorACPSetup(),
			&action.Request{
				Identity: testUtils.ClientIdentity(2),
				Request: `query {
					Users(order: {_alias: {sim: DESC}}, limit: 5) {
						name
						sim: SIMILARITY(vector: {vector: [1, 0, 0]})
					}
				}`,
				Results: map[string]any{"Users": vectorACPPublicResults},
			},
		),
	}

	testUtils.ExecuteTestCase(t, test)
}

// The cap is larger than the collection, so the index runs out of documents: without access control
// the short result would be the true answer and carry no warning. Here it warns anyway, since leaving
// it out would tell the caller the index holds fewer documents than the cap, hidden ones included.
func TestACP_VectorIndexQueryWithMaxCandidates_ShortResult_AlwaysWarns(t *testing.T) {
	test := testUtils.TestCase{
		Actions: append(vectorACPSetup(),
			&action.Request{
				Identity: testUtils.ClientIdentity(2),
				Request: `query {
					Users(order: {_alias: {sim: DESC}}, limit: 5) {
						name
						sim: SIMILARITY(vector: {vector: [1, 0, 0], maxCandidates: 100})
					}
				}`,
				Results: map[string]any{"Users": vectorACPPublicResults},
				ExpectedWarnings: []client.GQLWarning{{
					Code: client.WarningCodeVectorCandidateLimitReached,
					Detail: map[string]any{
						"field":         "vector",
						"limit":         5,
						"maxCandidates": 100,
					},
				}},
			},
		),
	}

	testUtils.ExecuteTestCase(t, test)
}

// The owner sees every document, so the index's nearest are the answer.
func TestACP_VectorIndexQuery_OwnerSeesNearest(t *testing.T) {
	test := testUtils.TestCase{
		Actions: append(vectorACPSetup(),
			&action.Request{
				Identity: testUtils.ClientIdentity(1),
				Request: `query {
					Users(order: {_alias: {sim: DESC}}, limit: 2) {
						name
						sim: SIMILARITY(vector: {vector: [1, 0, 0]})
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "private1", "sim": testUtils.CosineSimilarity([]float64{1, 0, 0}, []float64{1, 0, 0})},
						{"name": "private2", "sim": testUtils.CosineSimilarity([]float64{0.9, 0.1, 0}, []float64{1, 0, 0})},
					},
				},
			},
		),
	}

	testUtils.ExecuteTestCase(t, test)
}

// vectorACPIndexedFilterSetup is like vectorACPSetup with a filter field that has a secondary index,
// and optionally two documents only identity 1 can read that match the filter too.
func vectorACPIndexedFilterSetup(withHidden bool) []any {
	actions := []any{
		testUtils.AddDACPolicy{
			Identity: testUtils.ClientIdentity(1),
			Policy:   userPolicy,
		},
		&action.AddCollection{
			SDL: `
				type Users @policy(
					id: "{{.Policy0}}",
					resource: "users"
				) {
					name: String
					category: String @index
					vector: [Float32!] @index(vector: {dimensions: 3, hnsw: {metric: COSINE}})
				}
			`,
		},
	}
	if withHidden {
		actions = append(actions,
			&action.AddDoc{
				Identity: testUtils.ClientIdentity(1),
				DocMap:   map[string]any{"name": "private1", "category": "a", "vector": []float32{1, 0, 0}},
			},
			&action.AddDoc{
				Identity: testUtils.ClientIdentity(1),
				DocMap:   map[string]any{"name": "private2", "category": "a", "vector": []float32{0.9, 0.1, 0}},
			},
		)
	}
	return append(actions,
		&action.AddDoc{DocMap: map[string]any{"name": "public1", "category": "a", "vector": []float32{0.5, 0.5, 0}}},
		&action.AddDoc{DocMap: map[string]any{"name": "public2", "category": "a", "vector": []float32{0, 1, 0}}},
	)
}

// Whether the filter's secondary index lists its matches before the capped search stops depends on
// how many documents the search sees, hidden ones included. So the short result must warn either way:
// if it warned only without the hidden documents, the warning would tell the caller they exist.
func TestACP_VectorIndexQueryWithMaxCandidatesAndIndexedFilter_ShortResult_WarnsWhetherOrNotHiddenMatchesExist(t *testing.T) {
	for _, withHidden := range []bool{true, false} {
		t.Run(map[bool]string{true: "hidden matches", false: "no hidden matches"}[withHidden], func(t *testing.T) {
			test := testUtils.TestCase{
				Actions: append(vectorACPIndexedFilterSetup(withHidden),
					&action.Request{
						Identity: testUtils.ClientIdentity(2),
						Request: `query {
							Users(filter: {category: {_eq: "a"}}, order: {_alias: {sim: DESC}}, limit: 5) {
								name
								sim: SIMILARITY(vector: {vector: [1, 0, 0], maxCandidates: 100})
							}
						}`,
						Results: map[string]any{"Users": vectorACPPublicResults},
						ExpectedWarnings: []client.GQLWarning{{
							Code: client.WarningCodeVectorCandidateLimitReached,
							Detail: map[string]any{
								"field":         "vector",
								"limit":         5,
								"maxCandidates": 100,
							},
						}},
					},
				),
			}
			testUtils.ExecuteTestCase(t, test)
		})
	}
}
