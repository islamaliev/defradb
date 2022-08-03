// Copyright 2022 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package simple

import (
	"testing"

	testUtils "github.com/sourcenetwork/defradb/tests/integration"
)

func TestQuerySimpleWithGroupByNumberWithGroupLimit(t *testing.T) {
	test := testUtils.QueryTestCase{
		Description: "Simple query with group by number, no children, rendered, limited group",
		Query: `query {
					users(groupBy: [Age]) {
						Age
						_group(limit: 1) {
							Name
						}
					}
				}`,
		Docs: map[int][]string{
			0: {
				`{
					"Name": "John",
					"Age": 32
				}`,
				`{
					"Name": "Bob",
					"Age": 32
				}`,
				`{
					"Name": "Alice",
					"Age": 19
				}`,
			},
		},
		Results: []map[string]interface{}{
			{
				"Age": uint64(32),
				"_group": []map[string]interface{}{
					{
						"Name": "Bob",
					},
				},
			},
			{
				"Age": uint64(19),
				"_group": []map[string]interface{}{
					{
						"Name": "Alice",
					},
				},
			},
		},
	}

	executeTestCase(t, test)
}

func TestQuerySimpleWithGroupByNumberWithMultipleGroupsWithDifferentLimits(t *testing.T) {
	test := testUtils.QueryTestCase{
		Description: "Simple query with group by number, no children, multiple rendered, limited groups",
		Query: `query {
					users(groupBy: [Age]) {
						Age
						G1: _group(limit: 1) {
							Name
						}
						G2: _group(limit: 2) {
							Name
						}
					}
				}`,
		Docs: map[int][]string{
			0: {
				`{
					"Name": "John",
					"Age": 32
				}`,
				`{
					"Name": "Bob",
					"Age": 32
				}`,
				`{
					"Name": "Alice",
					"Age": 19
				}`,
			},
		},
		Results: []map[string]interface{}{
			{
				"Age": uint64(32),
				"G1": []map[string]interface{}{
					{
						"Name": "Bob",
					},
				},
				"G2": []map[string]interface{}{
					{
						"Name": "Bob",
					},
					{
						"Name": "John",
					},
				},
			},
			{
				"Age": uint64(19),
				"G1": []map[string]interface{}{
					{
						"Name": "Alice",
					},
				},
				"G2": []map[string]interface{}{
					{
						"Name": "Alice",
					},
				},
			},
		},
	}

	executeTestCase(t, test)
}

func TestQuerySimpleWithGroupByNumberWithLimitAndGroupWithHigherLimit(t *testing.T) {
	test := testUtils.QueryTestCase{
		Description: "Simple query with group by number and limit, no children, rendered, limited group",
		Query: `query {
					users(groupBy: [Age], limit: 1) {
						Age
						_group(limit: 2) {
							Name
						}
					}
				}`,
		Docs: map[int][]string{
			0: {
				`{
					"Name": "John",
					"Age": 32
				}`,
				`{
					"Name": "Bob",
					"Age": 32
				}`,
				`{
					"Name": "Alice",
					"Age": 19
				}`,
			},
		},
		Results: []map[string]interface{}{
			{
				"Age": uint64(32),
				"_group": []map[string]interface{}{
					{
						"Name": "Bob",
					},
					{
						"Name": "John",
					},
				},
			},
		},
	}

	executeTestCase(t, test)
}

func TestQuerySimpleWithGroupByNumberWithLimitAndGroupWithLowerLimit(t *testing.T) {
	test := testUtils.QueryTestCase{
		Description: "Simple query with group by number and limit, no children, rendered, limited group",
		Query: `query {
					users(groupBy: [Age], limit: 2) {
						Age
						_group(limit: 1) {
							Name
						}
					}
				}`,
		Docs: map[int][]string{
			0: {
				`{
					"Name": "John",
					"Age": 32
				}`,
				`{
					"Name": "Bob",
					"Age": 32
				}`,
				`{
					"Name": "Alice",
					"Age": 19
				}`,
				`{
					"Name": "Alice",
					"Age": 42
				}`,
			},
		},
		Results: []map[string]interface{}{
			{
				"Age": uint64(32),
				"_group": []map[string]interface{}{
					{
						"Name": "Bob",
					},
				},
			},
			{
				"Age": uint64(42),
				"_group": []map[string]interface{}{
					{
						"Name": "Alice",
					},
				},
			},
		},
	}

	executeTestCase(t, test)
}