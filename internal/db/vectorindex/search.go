// Copyright 2026 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package vectorindex is the DefraDB side of a vector index. It exposes an algorithm-agnostic
// surface — Open returns a handle the write path maintains, and Search runs a nearest-neighbour
// query for the read path — and selects the algorithm from the index description. Neither caller
// needs to know which algorithm backs the index or its parameters.
//
// Only the hnsw_*.go files touch the vector engine (internal/index/hnsw); everything else is
// engine-agnostic. That split is the seam for a possible future engine extraction, and the point
// where a second algorithm (IVFFlat) would slot in.
package vectorindex

import (
	"context"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/internal/db/id"
)

// SearchResult is one vector-search hit. A smaller Distance is nearer.
type SearchResult struct {
	DocID    string
	Distance float64
}

// Search returns up to k documents nearest to query, nearest first, reading through the transaction
// on ctx.
//
// A hit whose short id no longer maps to a document is skipped: the document can be deleted in this
// same transaction after the graph was read, and a dangling id must not reach the caller.
//
// exhausted reports that the index returned fewer than k hits, so a larger k would find nothing
// more. It is judged on the index's own hits, before dangling ids are skipped, so a skipped id does
// not make the index look smaller than it is. Like the search itself it is approximate: a graph that
// cannot reach some of its documents from the entry point reports them missing too.
func Search(
	ctx context.Context,
	collectionShortID, indexID, epoch uint32,
	desc client.VectorIndexDescription,
	query []float32,
	k int,
) (results []SearchResult, exhausted bool, err error) {
	index, err := Open(ctx, collectionShortID, indexID, epoch, desc)
	if err != nil {
		return nil, false, err
	}

	hits, err := index.Search(query, k)
	if err != nil {
		return nil, false, err
	}

	results = make([]SearchResult, 0, len(hits))
	for _, hit := range hits {
		docID, found, err := id.GetDocID(ctx, hit.NodeID)
		if err != nil {
			return nil, false, err
		}
		if !found {
			continue
		}
		results = append(results, SearchResult{DocID: docID, Distance: hit.Distance})
	}
	return results, len(hits) < k, nil
}

// Hit is one search hit: the indexed node id (a document short id) and its distance to the query
// (smaller is nearer). It is the algorithm-agnostic result every binding returns.
type Hit struct {
	NodeID   uint64
	Distance float64
}

// Index is a handle to a vector index, driven by node id without knowing the underlying algorithm.
// The write path uses Insert and Delete; Search backs the package-level Search above. Node ids are
// document short ids.
type Index interface {
	Insert(nodeID uint64, vec []float32) error
	Delete(nodeID uint64) error
	Search(query []float32, k int) ([]Hit, error)
}

// Open builds the index for desc over the given keyspace, selected by algorithm. An unsupported
// algorithm is an error, so a mis-configured index fails when it is opened rather than on first use.
// A second algorithm adds a case here returning its own Index implementation.
func Open(
	ctx context.Context,
	collectionShortID, indexID, epoch uint32,
	desc client.VectorIndexDescription,
) (Index, error) {
	switch desc.Algorithm {
	case client.VectorAlgorithmHNSW:
		index, err := openHNSW(ctx, collectionShortID, indexID, epoch, desc)
		if err != nil {
			// Return a nil interface, not a non-nil interface wrapping a nil pointer.
			return nil, err
		}
		return index, nil
	default:
		return nil, newErrUnsupportedAlgorithm(desc.Algorithm)
	}
}
