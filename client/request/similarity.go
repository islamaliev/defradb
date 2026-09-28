// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package request

import "github.com/sourcenetwork/immutable"

// Similarity is a functional field that defines the
// parameters to calculate the cosine similarity between two vectors.
type Similarity struct {
	Field
	// Vector contains the vector to compare the target field to.
	//
	// It will be of type Int, Float32 or Float64. It must be the same type and length as Target.
	Vector any

	// Target is the field in the host object that we will compare the vector to.
	//
	// It must be a field of type Int, Float32 or Float64. It must be the same type and length as Vector.
	Target string

	// MaxCandidates caps how many documents the target's vector index may examine to answer a
	// filtered nearest-neighbour query. Without it the complete answer is always returned; with it
	// the result can be short, which is reported as a warning.
	MaxCandidates immutable.Option[uint64]
}
