// Copyright 2024 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package client

import (
	"time"

	"github.com/sourcenetwork/immutable"
	"golang.org/x/exp/constraints"
)

type baseNillableNormalValue[T any] struct {
	baseNormalValue[immutable.Option[T]]
}

func (v baseNillableNormalValue[T]) Unwrap() any {
	if v.val.HasValue() {
		return v.val.Value()
	}
	return nil
}

func (v baseNillableNormalValue[T]) IsNil() bool {
	return !v.val.HasValue()
}

func (v baseNillableNormalValue[T]) IsNillable() bool {
	return true
}

func newBaseNillableNormalValue[T any](val immutable.Option[T], kind FieldKind) baseNillableNormalValue[T] {
	return baseNillableNormalValue[T]{newBaseNormalValue(val, kind)}
}

type normalNillableBool struct {
	baseNillableNormalValue[bool]
}

func (v normalNillableBool) NillableBool() (immutable.Option[bool], bool) {
	return v.val, true
}

type normalNillableInt struct {
	baseNillableNormalValue[int64]
}

func (v normalNillableInt) NillableInt() (immutable.Option[int64], bool) {
	return v.val, true
}

type normalNillableFloat struct {
	baseNillableNormalValue[float64]
}

func (v normalNillableFloat) NillableFloat() (immutable.Option[float64], bool) {
	return v.val, true
}

type normalNillableString struct {
	baseNillableNormalValue[string]
}

func (v normalNillableString) NillableString() (immutable.Option[string], bool) {
	return v.val, true
}

type normalNillableBytes struct {
	baseNillableNormalValue[[]byte]
}

func (v normalNillableBytes) NillableBytes() (immutable.Option[[]byte], bool) {
	return v.val, true
}

type normalNillableTime struct {
	baseNillableNormalValue[time.Time]
}

func (v normalNillableTime) NillableTime() (immutable.Option[time.Time], bool) {
	return v.val, true
}

type normalNillableDocument struct {
	baseNillableNormalValue[*Document]
}

func (v normalNillableDocument) NillableDocument() (immutable.Option[*Document], bool) {
	return v.val, true
}

// NewNormalNillableBool creates a new NormalValue that represents a `immutable.Option[bool]` value.
func NewNormalNillableBool(val immutable.Option[bool]) NormalValue {
	return normalNillableBool{newBaseNillableNormalValue(val, FieldKind_NILLABLE_BOOL)}
}

// NewNormalNillableInt creates a new NormalValue that represents a `immutable.Option[int64]` value.
func NewNormalNillableInt[T constraints.Integer | constraints.Float](val immutable.Option[T]) NormalValue {
	return normalNillableInt{newBaseNillableNormalValue(normalizeNillableNum[int64](val), FieldKind_NILLABLE_INT)}
}

// NewNormalNillableFloat creates a new NormalValue that represents a `immutable.Option[float64]` value.
func NewNormalNillableFloat[T constraints.Integer | constraints.Float](val immutable.Option[T]) NormalValue {
	return normalNillableFloat{newBaseNillableNormalValue(normalizeNillableNum[float64](val), FieldKind_NILLABLE_FLOAT)}
}

// NewNormalNillableString creates a new NormalValue that represents a `immutable.Option[string]` value.
func NewNormalNillableString[T string | []byte](val immutable.Option[T]) NormalValue {
	return normalNillableString{
		newBaseNillableNormalValue(normalizeNillableChars[string](val), FieldKind_NILLABLE_STRING),
	}
}

// NewNormalNillableJSON creates a new NormalValue that represents nillable JSON a `immutable.Option[string]` value.
func NewNormalNillableJSON[T string | []byte](val immutable.Option[T]) NormalValue {
	return normalNillableString{
		newBaseNillableNormalValue(normalizeNillableChars[string](val), FieldKind_NILLABLE_JSON),
	}
}

// NewNormalNillableBytes creates a new NormalValue that represents a `immutable.Option[[]byte]` value.
func NewNormalNillableBytes[T string | []byte](val immutable.Option[T]) NormalValue {
	return normalNillableBytes{
		newBaseNillableNormalValue(normalizeNillableChars[[]byte](val), FieldKind_NILLABLE_BLOB),
	}
}

// NewNormalNillableTime creates a new NormalValue that represents a `immutable.Option[time.Time]` value.
func NewNormalNillableTime(val immutable.Option[time.Time]) NormalValue {
	return normalNillableTime{newBaseNillableNormalValue(val, FieldKind_NILLABLE_DATETIME)}
}

// NewNormalNillableDocument creates a new NormalValue that represents a `immutable.Option[*Document]` value.
func NewNormalNillableDocument(val immutable.Option[*Document], kind ObjectKind) NormalValue {
	return normalNillableDocument{newBaseNillableNormalValue(val, kind)}
}

func normalizeNillableNum[R int64 | float64, T constraints.Integer | constraints.Float](
	val immutable.Option[T],
) immutable.Option[R] {
	if val.HasValue() {
		return immutable.Some(R(val.Value()))
	}
	return immutable.None[R]()
}

func normalizeNillableChars[R string | []byte, T string | []byte](val immutable.Option[T]) immutable.Option[R] {
	if val.HasValue() {
		return immutable.Some(R(val.Value()))
	}
	return immutable.None[R]()
}
