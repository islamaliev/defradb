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
	"testing"

	"github.com/sourcenetwork/immutable"
	"github.com/sourcenetwork/lens/host-go/config/model"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/tests/action"
	testUtils "github.com/sourcenetwork/defradb/tests/integration"
	"github.com/sourcenetwork/defradb/tests/lenses"
)

// vectorWarningSetup builds a collection with a cosine vector index and four documents. Every test
// here uses the same data and differs only in the shape of the request.
func vectorWarningSetup() []any {
	return []any{
		&action.AddCollection{
			SDL: `type User {
				name: String
				age: Int
				vector: [Float32!] @index(vector: {dimensions: 3, hnsw: {metric: COSINE}})
			}`,
		},
		&action.AddDoc{DocMap: map[string]any{"name": "x", "age": 10, "vector": []float32{1, 0, 0}}},
		&action.AddDoc{DocMap: map[string]any{"name": "y", "age": 20, "vector": []float32{0, 1, 0}}},
		&action.AddDoc{DocMap: map[string]any{"name": "xy", "age": 30, "vector": []float32{0.9, 0.4, 0}}},
		&action.AddDoc{DocMap: map[string]any{"name": "z", "age": 40, "vector": []float32{0, 0, 1}}},
	}
}

// unusedIndexWarning builds the warning a fallback is expected to report.
func unusedIndexWarning(reason string) []client.GQLWarning {
	return []client.GQLWarning{
		{
			Code: client.WarningCodeVectorIndexUnused,
			Detail: map[string]any{
				"field":  "vector",
				"reason": reason,
			},
		},
	}
}

// The control. Without it, tests that only check the fallbacks would pass even if every query
// warned.
func TestVectorIndexWarning_QueryUsesIndex_ReportsNoWarning(t *testing.T) {
	test := testUtils.TestCase{
		Actions: append(vectorWarningSetup(),
			&action.Request{
				Request: `query {
					User(order: {_alias: {sim: DESC}}, limit: 2){
						name
						sim: SIMILARITY(vector: {vector: [1, 0, 0]})
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{"name": "x", "sim": testUtils.CosineSimilarity([]float64{1, 0, 0}, []float64{1, 0, 0})},
						{"name": "xy", "sim": testUtils.CosineSimilarity([]float64{0.9, 0.4, 0}, []float64{1, 0, 0})},
					},
				},
			},
		),
	}

	testUtils.ExecuteTestCase(t, test)
}

// Ascending asks for the farthest documents, which the graph cannot serve. This is the easiest
// mistake to make: anyone thinking in distances rather than similarities reaches for ASC.
func TestVectorIndexWarning_OrderedAscending_ReportsWarning(t *testing.T) {
	test := testUtils.TestCase{
		Actions: append(vectorWarningSetup(),
			&action.Request{
				Request: `query {
					User(order: {_alias: {sim: ASC}}, limit: 2){
						name
						sim: SIMILARITY(vector: {vector: [1, 0, 0]})
					}
				}`,
				// "y" and "z" both score 0, so their relative order is not defined.
				NonOrderedResults: true,
				Results: map[string]any{
					"User": []map[string]any{
						{"name": "z", "sim": testUtils.CosineSimilarity([]float64{0, 0, 1}, []float64{1, 0, 0})},
						{"name": "y", "sim": testUtils.CosineSimilarity([]float64{0, 1, 0}, []float64{1, 0, 0})},
					},
				},
				ExpectedWarnings: unusedIndexWarning("notOrderedBySimilarityDesc"),
			},
		),
	}

	testUtils.ExecuteTestCase(t, test)
}

// Without a limit there is no k for the graph to return, so the whole collection is read.
func TestVectorIndexWarning_NoLimit_ReportsWarning(t *testing.T) {
	test := testUtils.TestCase{
		Actions: append(vectorWarningSetup(),
			&action.Request{
				Request: `query {
					User(order: {_alias: {sim: DESC}}){
						name
						sim: SIMILARITY(vector: {vector: [1, 0, 0]})
					}
				}`,
				// "y" and "z" both score 0, so their relative order is not defined.
				NonOrderedResults: true,
				Results: map[string]any{
					"User": []map[string]any{
						{"name": "x", "sim": testUtils.CosineSimilarity([]float64{1, 0, 0}, []float64{1, 0, 0})},
						{"name": "xy", "sim": testUtils.CosineSimilarity([]float64{0.9, 0.4, 0}, []float64{1, 0, 0})},
						{"name": "y", "sim": testUtils.CosineSimilarity([]float64{0, 1, 0}, []float64{1, 0, 0})},
						{"name": "z", "sim": testUtils.CosineSimilarity([]float64{0, 0, 1}, []float64{1, 0, 0})},
					},
				},
				ExpectedWarnings: unusedIndexWarning("noLimit"),
			},
		),
	}

	testUtils.ExecuteTestCase(t, test)
}

// A filter used to rule the index out (https://github.com/sourcenetwork/defradb/issues/5071). The
// query now searches the index for more candidates than the limit until enough pass, so it no longer
// reads the whole collection and no longer warns.
func TestVectorIndexWarning_WithFilter_UsesIndexAndReportsNoWarning(t *testing.T) {
	req := `query {
		User(filter: {age: {_gt: 15}}, order: {_alias: {sim: DESC}}, limit: 2){
			name
			sim: SIMILARITY(vector: {vector: [1, 0, 0]})
		}
	}`
	test := testUtils.TestCase{
		Actions: append(vectorWarningSetup(),
			&action.Request{
				Request: req,
				Results: map[string]any{
					"User": []map[string]any{
						{"name": "xy", "sim": testUtils.CosineSimilarity([]float64{0.9, 0.4, 0}, []float64{1, 0, 0})},
						{"name": "y", "sim": testUtils.CosineSimilarity([]float64{0, 1, 0}, []float64{1, 0, 0})},
					},
				},
			},
			&action.Request{
				Request:  makeExplainQuery(req),
				Asserter: testUtils.NewExplainAsserter().WithVectorStrategy("overFetch"),
			},
		),
	}

	testUtils.ExecuteTestCase(t, test)
}

// Filtering the k nearest after the fact can reject all of them and return nothing while matching
// documents exist. Only "z" matches age > 35 and it is the farthest from the query, so asking the
// graph for k=1 would find "x", drop it, and return an empty result. The index is searched further
// until "z" is found instead.
func TestVectorIndexWarning_SelectiveFilter_StillReturnsMatchingDocs(t *testing.T) {
	req := `query {
		User(filter: {age: {_gt: 35}}, order: {_alias: {sim: DESC}}, limit: 1){
			name
			sim: SIMILARITY(vector: {vector: [1, 0, 0]})
		}
	}`
	test := testUtils.TestCase{
		Actions: append(vectorWarningSetup(),
			&action.Request{
				Request: req,
				Results: map[string]any{
					"User": []map[string]any{
						{"name": "z", "sim": testUtils.CosineSimilarity([]float64{0, 0, 1}, []float64{1, 0, 0})},
					},
				},
			},
			&action.Request{
				Request:  makeExplainQuery(req),
				Asserter: testUtils.NewExplainAsserter().WithVectorStrategy("overFetch"),
			},
		),
	}

	testUtils.ExecuteTestCase(t, test)
}

// Fewer documents match than the limit, so the index alone cannot say there are no others: the whole
// collection is read, and the query says why.
func TestVectorIndexWarning_FilterPassesFewerThanLimit_ReportsWarning(t *testing.T) {
	req := `query {
		User(filter: {age: {_gt: 35}}, order: {_alias: {sim: DESC}}, limit: 2){
			name
			sim: SIMILARITY(vector: {vector: [1, 0, 0]})
		}
	}`
	test := testUtils.TestCase{
		Actions: append(vectorWarningSetup(),
			&action.Request{
				Request: req,
				Results: map[string]any{
					"User": []map[string]any{
						{"name": "z", "sim": testUtils.CosineSimilarity([]float64{0, 0, 1}, []float64{1, 0, 0})},
					},
				},
				ExpectedWarnings: unusedIndexWarning("filterTooSelective"),
			},
			&action.Request{
				Request: makeExplainQuery(req),
				// The index was still searched before giving up: first for twice the limit, then for
				// more, when it turned out to hold all four documents.
				Asserter:         testUtils.NewExplainAsserter().WithVectorStrategy("").WithIndexFetches(2),
				ExpectedWarnings: unusedIndexWarning("filterTooSelective"),
			},
		),
	}

	testUtils.ExecuteTestCase(t, test)
}

// A document without a vector is never in the index, but it still matches the filter, so reading the
// whole collection is what finds it. Returning only what the index holds would leave it out.
func TestVectorIndexWarning_FilterPassesFewerThanLimit_ReturnsDocWithoutVector(t *testing.T) {
	test := testUtils.TestCase{
		Actions: append(vectorWarningSetup(),
			&action.AddDoc{DocMap: map[string]any{"name": "noVector", "age": 50}},
			&action.Request{
				Request: `query {
					User(filter: {age: {_gt: 35}}, order: {_alias: {sim: DESC}}, limit: 5){
						name
						sim: SIMILARITY(vector: {vector: [1, 0, 0]})
					}
				}`,
				// Both score 0, so their relative order is not defined.
				NonOrderedResults: true,
				Results: map[string]any{
					"User": []map[string]any{
						{"name": "z", "sim": testUtils.CosineSimilarity([]float64{0, 0, 1}, []float64{1, 0, 0})},
						{"name": "noVector", "sim": float64(0)},
					},
				},
				ExpectedWarnings: unusedIndexWarning("filterTooSelective"),
			},
		),
	}

	testUtils.ExecuteTestCase(t, test)
}

// An alias is only computed after the scan, so the scan cannot tell which of the index's documents
// pass the filter, and the whole collection is read.
func TestVectorIndexWarning_FilterOnAlias_ReportsWarning(t *testing.T) {
	test := testUtils.TestCase{
		Actions: append(vectorWarningSetup(),
			&action.Request{
				Request: `query {
					User(filter: {_alias: {sim: {_gt: 0.5}}}, order: {_alias: {sim: DESC}}, limit: 2){
						name
						sim: SIMILARITY(vector: {vector: [1, 0, 0]})
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{"name": "x", "sim": testUtils.CosineSimilarity([]float64{1, 0, 0}, []float64{1, 0, 0})},
						{"name": "xy", "sim": testUtils.CosineSimilarity([]float64{0.9, 0.4, 0}, []float64{1, 0, 0})},
					},
				},
				ExpectedWarnings: unusedIndexWarning("filter"),
			},
		),
	}

	testUtils.ExecuteTestCase(t, test)
}

// A related document's fields are checked after the join, so the scan cannot tell which of the index's
// documents pass, and the whole collection is read. The condition is a negation on purpose: checked at
// the scan, where the related document is not loaded, it would pass every document, so the query
// would stop at "x" and "xy" (the nearest) only for the join to drop both and return nothing.
func TestVectorIndexWarning_FilterOnRelatedDocument_ReportsWarning(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddCollection{
				SDL: `
					type Team {
						name: String
						users: [User]
					}

					type User {
						name: String
						team: Team
						vector: [Float32!] @index(vector: {dimensions: 3, hnsw: {metric: COSINE}})
					}
				`,
			},
			&action.AddDoc{CollectionID: 0, DocMap: map[string]any{"name": "near"}},
			&action.AddDoc{CollectionID: 0, DocMap: map[string]any{"name": "far"}},
			&action.AddDoc{CollectionID: 1, DocMap: map[string]any{
				"name": "x", "team": testUtils.NewDocIndex(0, 1), "vector": []float32{1, 0, 0},
			}},
			&action.AddDoc{CollectionID: 1, DocMap: map[string]any{
				"name": "xy", "team": testUtils.NewDocIndex(0, 1), "vector": []float32{0.9, 0.4, 0},
			}},
			&action.AddDoc{CollectionID: 1, DocMap: map[string]any{
				"name": "y", "team": testUtils.NewDocIndex(0, 0), "vector": []float32{0, 1, 0},
			}},
			&action.Request{
				Request: `query {
					User(filter: {team: {name: {_neq: "far"}}}, order: {_alias: {sim: DESC}}, limit: 1){
						name
						sim: SIMILARITY(vector: {vector: [1, 0, 0]})
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{"name": "y", "sim": testUtils.CosineSimilarity([]float64{0, 1, 0}, []float64{1, 0, 0})},
					},
				},
				ExpectedWarnings: unusedIndexWarning("filter"),
			},
		},
	}

	testUtils.ExecuteTestCase(t, test)
}

// A deleted document is removed from the index, so the index cannot find the deleted documents
// showDeleted asks for, and the whole collection is read. Here the deleted "x" is the nearest match.
func TestVectorIndexWarning_ShowDeleted_ReportsWarning(t *testing.T) {
	for _, filter := range []string{"", "filter: {age: {_lt: 35}}, "} {
		t.Run(map[bool]string{true: "without filter", false: "with filter"}[filter == ""], func(t *testing.T) {
			test := testUtils.TestCase{
				Actions: append(vectorWarningSetup(),
					testUtils.DeleteDoc{DocID: 0},
					&action.Request{
						Request: `query {
							User(` + filter + `showDeleted: true, order: {_alias: {sim: DESC}}, limit: 1){
								name
								sim: SIMILARITY(vector: {vector: [1, 0, 0]})
							}
						}`,
						Results: map[string]any{
							"User": []map[string]any{
								{"name": "x", "sim": testUtils.CosineSimilarity([]float64{1, 0, 0}, []float64{1, 0, 0})},
							},
						},
						ExpectedWarnings: unusedIndexWarning("showDeleted"),
					},
				),
			}
			testUtils.ExecuteTestCase(t, test)
		})
	}
}

// The limit counts groups, but the nearest documents can all fall in one group: here the three nearest
// all have age 10. Narrowing to them would return one group where two are asked for.
func TestVectorIndexWarning_GroupBy_ReportsWarning(t *testing.T) {
	test := testUtils.TestCase{
		Actions: append(vectorWarningSetup(),
			&action.AddDoc{DocMap: map[string]any{"name": "x2", "age": 10, "vector": []float32{0.95, 0.1, 0}}},
			&action.AddDoc{DocMap: map[string]any{"name": "x3", "age": 10, "vector": []float32{0.97, 0.05, 0}}},
			&action.Request{
				Request: `query {
					User(groupBy: [age], filter: {age: {_gt: 5}}, order: {_alias: {sim: DESC}}, limit: 2) {
						age
						sim: SIMILARITY(vector: {vector: [1, 0, 0]})
						GROUP { name }
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{
							"age":   int64(10),
							"sim":   testUtils.CosineSimilarity([]float64{0.97, 0.05, 0}, []float64{1, 0, 0}),
							"GROUP": []map[string]any{{"name": "x"}, {"name": "x2"}, {"name": "x3"}},
						},
						{
							"age":   int64(30),
							"sim":   testUtils.CosineSimilarity([]float64{0.9, 0.4, 0}, []float64{1, 0, 0}),
							"GROUP": []map[string]any{{"name": "xy"}},
						},
					},
				},
				ExpectedWarnings: unusedIndexWarning("groupBy"),
			},
		),
	}

	testUtils.ExecuteTestCase(t, test)
}

// A relation's documents are narrowed to each parent's only when the join runs, so the collection's
// nearest documents are not the ones asked for: here they all belong to team "a", and team "b" would
// get none. With and without a filter, each team must get its own nearest.
func TestVectorIndexWarning_NestedSelect_ReportsWarning(t *testing.T) {
	for _, filter := range []string{"", "filter: {age: {_gt: 15}}, "} {
		t.Run(map[bool]string{true: "without filter", false: "with filter"}[filter == ""], func(t *testing.T) {
			test := testUtils.TestCase{
				Actions: []any{
					&action.AddCollection{
						SDL: `
							type Team {
								name: String
								users: [User]
							}

							type User {
								name: String
								age: Int
								team: Team
								vector: [Float32!] @index(vector: {dimensions: 3, hnsw: {metric: COSINE}})
							}
						`,
					},
					&action.AddDoc{CollectionID: 0, DocMap: map[string]any{"name": "a"}},
					&action.AddDoc{CollectionID: 0, DocMap: map[string]any{"name": "b"}},
					&action.AddDoc{CollectionID: 1, DocMap: map[string]any{
						"name": "a1", "age": 20, "team": testUtils.NewDocIndex(0, 0), "vector": []float32{1, 0, 0},
					}},
					&action.AddDoc{CollectionID: 1, DocMap: map[string]any{
						"name": "a2", "age": 20, "team": testUtils.NewDocIndex(0, 0), "vector": []float32{0.9, 0.1, 0},
					}},
					&action.AddDoc{CollectionID: 1, DocMap: map[string]any{
						"name": "b1", "age": 20, "team": testUtils.NewDocIndex(0, 1), "vector": []float32{0, 1, 0},
					}},
					&action.Request{
						Request: `query {
							Team(order: {name: ASC}) {
								name
								users(` + filter + `order: {_alias: {sim: DESC}}, limit: 1) {
									name
									sim: SIMILARITY(vector: {vector: [1, 0, 0]})
								}
							}
						}`,
						Results: map[string]any{
							"Team": []map[string]any{
								{
									"name": "a",
									"users": []map[string]any{
										{"name": "a1", "sim": testUtils.CosineSimilarity([]float64{1, 0, 0}, []float64{1, 0, 0})},
									},
								},
								{
									"name": "b",
									"users": []map[string]any{
										{"name": "b1", "sim": testUtils.CosineSimilarity([]float64{0, 1, 0}, []float64{1, 0, 0})},
									},
								},
							},
						},
						ExpectedWarnings: unusedIndexWarning("nestedSelect"),
					},
				},
			}
			testUtils.ExecuteTestCase(t, test)
		})
	}
}

// With lens migrations the filter is applied again to the migrated documents, so the scan's check on
// the stored ones cannot decide the result. The migration adds 100 to age: stored, every age passes
// age < 115, but migrated only "x" (110) does. Deciding at the scan would stop at "z" and "y", the
// nearest, and return nothing once the migrated filter dropped them.
func TestVectorIndexWarning_FilterOnMigratedCollection_ReportsWarning(t *testing.T) {
	test := testUtils.TestCase{
		Actions: append(vectorWarningSetup(),
			&action.PatchCollection{
				Patch: `
					[
						{ "op": "add", "path": "/User/Fields/-", "value": {"Name": "email", "Kind": "String"} }
					]
				`,
				Lens: immutable.Some(model.Lens{
					Lenses: []model.LensModule{
						{
							Path: lenses.IncrementModulePath,
							Arguments: map[string]any{
								"field": "age",
								"value": 100,
							},
						},
					},
				}),
			},
			&action.Request{
				Request: `query {
					User(filter: {age: {_lt: 115}}, order: {_alias: {sim: DESC}}, limit: 1){
						name
						sim: SIMILARITY(vector: {vector: [0, 0.3, 1]})
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{"name": "x", "sim": testUtils.CosineSimilarity([]float64{1, 0, 0}, []float64{0, 0.3, 1})},
					},
				},
				ExpectedWarnings: unusedIndexWarning("filter"),
			},
		),
	}

	testUtils.ExecuteTestCase(t, test)
}

// With two similarity fields, which one drives the search is ambiguous, so the query full-scans.
func TestVectorIndexWarning_MultipleSimilarityFields_ReportsWarning(t *testing.T) {
	test := testUtils.TestCase{
		Actions: append(vectorWarningSetup(),
			&action.Request{
				Request: `query {
					User(order: {_alias: {sim: DESC}}, limit: 2){
						name
						sim: SIMILARITY(vector: {vector: [1, 0, 0]})
						other: SIMILARITY(vector: {vector: [0, 1, 0]})
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{
							"name":  "x",
							"sim":   testUtils.CosineSimilarity([]float64{1, 0, 0}, []float64{1, 0, 0}),
							"other": testUtils.CosineSimilarity([]float64{1, 0, 0}, []float64{0, 1, 0}),
						},
						{
							"name":  "xy",
							"sim":   testUtils.CosineSimilarity([]float64{0.9, 0.4, 0}, []float64{1, 0, 0}),
							"other": testUtils.CosineSimilarity([]float64{0.9, 0.4, 0}, []float64{0, 1, 0}),
						},
					},
				},
				ExpectedWarnings: unusedIndexWarning("multipleSimilarityFields"),
			},
		),
	}

	testUtils.ExecuteTestCase(t, test)
}

// Nothing to fall back from, so no warning. Otherwise every similarity query in a schema without
// vector indexes would report one.
func TestVectorIndexWarning_NoVectorIndexOnField_ReportsNoWarning(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddCollection{
				SDL: `type User {
					name: String
					vector: [Float32!]
				}`,
			},
			&action.AddDoc{DocMap: map[string]any{"name": "x", "vector": []float32{1, 0, 0}}},
			&action.AddDoc{DocMap: map[string]any{"name": "y", "vector": []float32{0, 1, 0}}},
			&action.Request{
				Request: `query {
					User(order: {_alias: {sim: ASC}}, limit: 1){
						name
						sim: SIMILARITY(vector: {vector: [1, 0, 0]})
					}
				}`,
				Results: map[string]any{
					"User": []map[string]any{
						{"name": "y", "sim": testUtils.CosineSimilarity([]float64{0, 1, 0}, []float64{1, 0, 0})},
					},
				},
			},
		},
	}

	testUtils.ExecuteTestCase(t, test)
}
