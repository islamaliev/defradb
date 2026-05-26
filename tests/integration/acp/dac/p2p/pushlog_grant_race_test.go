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

package test_acp_dac_p2p

import (
	"fmt"
	"testing"

	"github.com/sourcenetwork/immutable"

	"github.com/sourcenetwork/defradb/internal/debug"
	"github.com/sourcenetwork/defradb/tests/action"
	testUtils "github.com/sourcenetwork/defradb/tests/integration"
	"github.com/sourcenetwork/defradb/tests/state"
)

// enableTimelineForTest arms the global debug Timeline and prints it at
// test cleanup so we get a visual ordered log of significant P2P events.
// Only intended for the probe tests in this file.
func enableTimelineForTest(t *testing.T) {
	debug.DefaultTimeline.Enable()
	debug.DefaultTimeline.Log("TEST", "enabled")
	t.Cleanup(func() {
		fmt.Println()
		fmt.Println("═════════════════════════════════════════════════════════════════")
		fmt.Println("P2P / ACP grant-race timeline:")
		fmt.Println("═════════════════════════════════════════════════════════════════")
		fmt.Print(debug.DefaultTimeline.Render())
		fmt.Println("═════════════════════════════════════════════════════════════════")
		debug.DefaultTimeline.Disable()
	})
}

// Probes the AddReplicator (push) path under ACP. Receiver-side
// trySelfHasAccess is skipped (isReplicator=true), and the sender-side
// hasAccess filter short-circuits on the "peer is in replicators list"
// rule, so SourceHub is never consulted for the block-fetch authz
// decision. This is why this test passes on develop — it does not
// exercise the code path where the demess bug lives.
func TestACP_P2P_NonCreatorReadsReplicatedDocAfterGrantsLand_Probe(t *testing.T) {
	enableTimelineForTest(t)
	test := testUtils.TestCase{
		SupportedDocumentACPTypes: immutable.Some(
			[]state.DocumentACPType{
				state.SourceHubDocumentACPType,
			},
		),
		Actions: []any{
			testUtils.RandomNetworkingConfig(),
			testUtils.RandomNetworkingConfig(),

			testUtils.AddDACPolicy{
				Identity: testUtils.ClientIdentity(1),
				Policy: `
description: A Policy
name: Test Policy
resources:
- name: users
  permissions:
  - expr: deleter
    name: delete
  - expr: dummy
    name: nothing
  - expr: reader + updater + deleter
    name: read
  - expr: updater
    name: update
  relations:
  - manages:
    - reader
    - updater
    name: admin
    types:
    - actor
  - name: deleter
    types:
    - actor
  - name: dummy
    types:
    - actor
  - name: reader
    types:
    - actor
  - name: updater
    types:
    - actor
`,
			},

			&action.AddCollection{
				SDL: `
					type Users @policy(
						id: "{{.Policy0}}",
						resource: "users"
					) {
						name: String
						age: Int
					}
				`,
			},

			testUtils.AddReplicator{
				SourceNodeID: 0,
				TargetNodeID: 1,
			},

			// Doc is created BEFORE any grants for the eventual non-creator
			// reader or for the receiving node. This is the demess timeline:
			// the doc-create pubsub announce races ahead of the SourceHub
			// grant txs that follow.
			&action.AddDoc{
				Identity:     testUtils.ClientIdentity(1),
				NodeID:       immutable.Some(0),
				CollectionID: 0,
				Doc: `
					{
						"name": "Alice",
						"age": 30
					}
				`,
			},

			// Grant the non-creator reader.
			testUtils.AddDACActorRelationship{
				NodeID:            immutable.Some(0),
				RequestorIdentity: testUtils.ClientIdentity(1),
				TargetIdentity:    testUtils.ClientIdentity(2),
				CollectionID:      0,
				DocID:             0,
				Relation:          "reader",
				ExpectedExistence: false,
			},

			// Grant the receiving node (DGC-012).
			testUtils.AddDACActorRelationship{
				NodeID:            immutable.Some(0),
				RequestorIdentity: testUtils.ClientIdentity(1),
				TargetIdentity:    testUtils.NodeIdentity(1),
				CollectionID:      0,
				DocID:             0,
				Relation:          "reader",
				ExpectedExistence: false,
			},

			testUtils.WaitForSync{},

			// Owner can read on node 1 — confirms replication landed at all.
			&action.Request{
				Identity: testUtils.ClientIdentity(1),
				NodeID:   immutable.Some(1),
				Request: `
					query {
						Users {
							name
							age
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Alice", "age": int64(30)},
					},
				},
			},

			// The actual demess assertion: non-creator with reader grant
			// can see the doc on the receiving node.
			&action.Request{
				Identity: testUtils.ClientIdentity(2),
				NodeID:   immutable.Some(1),
				Request: `
					query {
						Users {
							name
							age
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Alice", "age": int64(30)},
					},
				},
			},
		},
	}

	testUtils.ExecuteTestCase(t, test)
}

// Probes the AddCollectionSubscription (pull) path under ACP — the
// demess production shape. The receiver subscribes to the collection
// on node 0; doc creates pubsub-announce; the receiver's pushlog
// handler fires; trySelfHasAccess runs; the receiver-side block fetch
// goes back through the sender's hasAccess filter, which DOES consult
// SourceHub (no replicator short-circuit on this path).
//
// Setup mirrors the demess timeline: doc is created on node 0 with no
// receiver-side grants, then grants land. If grant propagation races
// the block-fetch deadline, we should see a context-deadline-exceeded
// error here in CI, same as demess sees in prod.
func TestACP_P2P_SubscriptionPath_NonCreatorReadsAfterGrantsLand_Probe(t *testing.T) {
	enableTimelineForTest(t)
	test := testUtils.TestCase{
		SupportedDocumentACPTypes: immutable.Some(
			[]state.DocumentACPType{
				state.SourceHubDocumentACPType,
			},
		),
		Actions: []any{
			testUtils.RandomNetworkingConfig(),
			testUtils.RandomNetworkingConfig(),

			testUtils.AddDACPolicy{
				Identity: testUtils.ClientIdentity(1),
				Policy: `
description: A Policy
name: Test Policy
resources:
- name: users
  permissions:
  - expr: deleter
    name: delete
  - expr: dummy
    name: nothing
  - expr: reader + updater + deleter
    name: read
  - expr: updater
    name: update
  relations:
  - manages:
    - reader
    - updater
    name: admin
    types:
    - actor
  - name: deleter
    types:
    - actor
  - name: dummy
    types:
    - actor
  - name: reader
    types:
    - actor
  - name: updater
    types:
    - actor
`,
			},

			&action.AddCollection{
				SDL: `
					type Users @policy(
						id: "{{.Policy0}}",
						resource: "users"
					) {
						name: String
						age: Int
					}
				`,
			},

			// Pull-based: receiver subscribes to the collection.
			testUtils.ConnectPeers{
				SourceNodeID: 1,
				TargetNodeID: 0,
			},
			testUtils.AddCollectionSubscription{
				NodeID:        1,
				CollectionIDs: []int{0},
			},

			// Doc created BEFORE any non-creator grants.
			&action.AddDoc{
				Identity:     testUtils.ClientIdentity(1),
				NodeID:       immutable.Some(0),
				CollectionID: 0,
				Doc: `
					{
						"name": "Alice",
						"age": 30
					}
				`,
			},

			// Grant the non-creator reader.
			testUtils.AddDACActorRelationship{
				NodeID:            immutable.Some(0),
				RequestorIdentity: testUtils.ClientIdentity(1),
				TargetIdentity:    testUtils.ClientIdentity(2),
				CollectionID:      0,
				DocID:             0,
				Relation:          "reader",
				ExpectedExistence: false,
			},

			// Grant the receiving node (DGC-012).
			testUtils.AddDACActorRelationship{
				NodeID:            immutable.Some(0),
				RequestorIdentity: testUtils.ClientIdentity(1),
				TargetIdentity:    testUtils.NodeIdentity(1),
				CollectionID:      0,
				DocID:             0,
				Relation:          "reader",
				ExpectedExistence: false,
			},

			testUtils.WaitForSync{},

			// Owner can read on node 1 — confirms replication landed at all.
			&action.Request{
				Identity: testUtils.ClientIdentity(1),
				NodeID:   immutable.Some(1),
				Request: `
					query {
						Users {
							name
							age
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Alice", "age": int64(30)},
					},
				},
			},

			// Non-creator with reader grant can see the doc on the receiver.
			&action.Request{
				Identity: testUtils.ClientIdentity(2),
				NodeID:   immutable.Some(1),
				Request: `
					query {
						Users {
							name
							age
						}
					}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"name": "Alice", "age": int64(30)},
					},
				},
			},
		},
	}

	testUtils.ExecuteTestCase(t, test)
}
