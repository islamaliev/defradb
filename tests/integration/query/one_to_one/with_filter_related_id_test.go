// Copyright 2026 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package one_to_one

import (
	"testing"

	"github.com/sourcenetwork/defradb/tests/action"
	testUtils "github.com/sourcenetwork/defradb/tests/integration"
)

func TestQueryOneToOne_WithNullEqFilterOnSecondaryFKField_ReturnsOnlyOrphans(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":   "Orphan Book",
					"rating": 3.0,
				},
			},
			&action.AddDoc{
				CollectionID: 0,
				DocMap: map[string]any{
					"name":   "Linked Book",
					"rating": 4.5,
				},
			},
			&action.AddDoc{
				CollectionID: 1,
				DocMap: map[string]any{
					"name":      "John Grisham",
					"age":       65,
					"verified":  true,
					"published": testUtils.NewDocIndex(0, 1),
				},
			},
			&action.Request{
				Request: `query {
					Book(filter: {_authorID: {_eq: null}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Book": []map[string]any{
						{"name": "Orphan Book"},
					},
				},
			},
		},
	}

	executeTestCase(t, test)
}
