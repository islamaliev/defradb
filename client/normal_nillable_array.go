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

type baseNillableArrayNormalValue[T any] struct {
	baseArrayNormalValue[immutable.Option[T]]
}

func (v baseNillableArrayNormalValue[T]) Unwrap() any {
	if v.val.HasValue() {
		return v.val.Value()
	}
	return nil
}

func (v baseNillableArrayNormalValue[T]) IsNil() bool {
	return !v.val.HasValue()
}

func (v baseNillableArrayNormalValue[T]) IsNillable() bool {
	return true
}

func (v baseNillableArrayNormalValue[T]) IsArray() bool {
	return true
}

func newBaseNillableArrayNormalValue[T any](val immutable.Option[T], kind FieldKind) baseNillableArrayNormalValue[T] {
	return baseNillableArrayNormalValue[T]{newBaseArrayNormalValue(val, kind)}
}

type normalBoolNillableArray struct {
	baseNillableArrayNormalValue[[]bool]
}

func (v normalBoolNillableArray) BoolNillableArray() (immutable.Option[[]bool], bool) {
	return v.val, true
}

type normalIntNillableArray struct {
	baseNillableArrayNormalValue[[]int64]
}

func (v normalIntNillableArray) IntNillableArray() (immutable.Option[[]int64], bool) {
	return v.val, true
}

type normalFloatNillableArray struct {
	baseNillableArrayNormalValue[[]float64]
}

func (v normalFloatNillableArray) FloatNillableArray() (immutable.Option[[]float64], bool) {
	return v.val, true
}

type normalStringNillableArray struct {
	baseNillableArrayNormalValue[[]string]
}

func (v normalStringNillableArray) StringNillableArray() (immutable.Option[[]string], bool) {
	return v.val, true
}

type normalBytesNillableArray struct {
	baseNillableArrayNormalValue[[][]byte]
}

func (v normalBytesNillableArray) BytesNillableArray() (immutable.Option[[][]byte], bool) {
	return v.val, true
}

type normalTimeNillableArray struct {
	baseNillableArrayNormalValue[[]time.Time]
}

func (v normalTimeNillableArray) TimeNillableArray() (immutable.Option[[]time.Time], bool) {
	return v.val, true
}

type normalDocumentNillableArray struct {
	baseNillableArrayNormalValue[[]*Document]
}

func (v normalDocumentNillableArray) DocumentNillableArray() (immutable.Option[[]*Document], bool) {
	return v.val, true
}

// NewNormalBoolNillableArray creates a new NormalValue that represents a `immutable.Option[[]bool]` value.
func NewNormalBoolNillableArray(val immutable.Option[[]bool]) NormalValue {
	return normalBoolNillableArray{newBaseNillableArrayNormalValue(val, FieldKind_BOOL_NILLABLE_ARRAY)}
}

// NewNormalIntNillableArray creates a new NormalValue that represents a `immutable.Option[[]int64]` value.
func NewNormalIntNillableArray[T constraints.Integer | constraints.Float](val immutable.Option[[]T]) NormalValue {
	return normalIntNillableArray{
		newBaseNillableArrayNormalValue(normalizeNumNillableArr[int64](val), FieldKind_INT_NILLABLE_ARRAY),
	}
}

// NewNormalFloatNillableArray creates a new NormalValue that represents a `immutable.Option[[]float64]` value.
func NewNormalFloatNillableArray[T constraints.Integer | constraints.Float](val immutable.Option[[]T]) NormalValue {
	return normalFloatNillableArray{
		newBaseNillableArrayNormalValue(normalizeNumNillableArr[float64](val), FieldKind_FLOAT_NILLABLE_ARRAY),
	}
}

// NewNormalStringNillableArray creates a new NormalValue that represents a `immutable.Option[[]string]` value.
func NewNormalStringNillableArray[T string | []byte](val immutable.Option[[]T]) NormalValue {
	return normalStringNillableArray{
		newBaseNillableArrayNormalValue(normalizeCharsNillableArr[string](val), FieldKind_STRING_NILLABLE_ARRAY),
	}
}

// NewNormalJSONNillableArray creates a new NormalValue that represents a `immutable.Option[[]string]` value.
func NewNormalJSONNillableArray[T string | []byte](val immutable.Option[[]T]) NormalValue {
	return normalStringNillableArray{
		newBaseNillableArrayNormalValue(normalizeCharsNillableArr[string](val), FieldKind_JSON_NILLABLE_ARRAY),
	}
}

// NewNormalBytesNillableArray creates a new NormalValue that represents a `immutable.Option[[][]byte]` value.
func NewNormalBytesNillableArray[T string | []byte](val immutable.Option[[]T]) NormalValue {
	return normalBytesNillableArray{
		newBaseNillableArrayNormalValue(normalizeCharsNillableArr[[]byte](val), FieldKind_BLOB_NILLABLE_ARRAY),
	}
}

// NewNormalTimeNillableArray creates a new NormalValue that represents a `immutable.Option[[]time.Time]` value.
func NewNormalTimeNillableArray(val immutable.Option[[]time.Time]) NormalValue {
	return normalTimeNillableArray{newBaseNillableArrayNormalValue(val, FieldKind_DATETIME_NILLABLE_ARRAY)}
}

// NewNormalDocumentNillableArray creates a new NormalValue that represents a `immutable.Option[[]*Document]` value.
func NewNormalDocumentNillableArray(val immutable.Option[[]*Document], kind ObjectArrayKind) NormalValue {
	return normalDocumentNillableArray{newBaseNillableArrayNormalValue(val, kind)}
}

func normalizeNumNillableArr[R int64 | float64, T constraints.Integer | constraints.Float](
	val immutable.Option[[]T],
) immutable.Option[[]R] {
	if val.HasValue() {
		return immutable.Some(normalizeNumArr[R](val.Value()))
	}
	return immutable.None[[]R]()
}

func normalizeCharsNillableArr[R string | []byte, T string | []byte](val immutable.Option[[]T]) immutable.Option[[]R] {
	if val.HasValue() {
		return immutable.Some(normalizeCharsArr[R](val.Value()))
	}
	return immutable.None[[]R]()
}
