// Copyright 2022 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package commits

import (
	"testing"

	testUtils "github.com/sourcenetwork/defradb/tests/integration"
)

func TestQueryCommitsWithDocIDAndLinkCount(t *testing.T) {
	test := testUtils.TestCase{
		Description: "Simple latest commits query with docID and link count",
		Actions: []any{
			updateUserCollectionSchema(),
			testUtils.CreateDoc{
				CollectionID: 0,
				Doc: `{
						"name":	"John",
						"age":	21
					}`,
			},
			testUtils.Request{
				Request: `query {
						commits(docID: "bae-f54b9689-e06e-5e3a-89b3-f3aee8e64ca7") {
							cid
							_count(field: links)
						}
					}`,
				Results: []map[string]any{
					{
						"cid":    "bafybeihfw5lufgs7ygv45to5rqvt3xkecjgikoccjyx6y2i7lnaclmrcjm",
						"_count": 0,
					},
					{
						"cid":    "bafybeigmez6gtszsqx6aevzlanvpazhhezw5va4wizhqtqz5k4s2dqjb24",
						"_count": 0,
					},
					{
						"cid":    "bafybeiedu23doqe2nagdbmkvfyuouajnfxo7ezy57vbv34dqewhwbfg45u",
						"_count": 2,
					},
				},
			},
		},
	}

	testUtils.ExecuteTestCase(t, test)
}
