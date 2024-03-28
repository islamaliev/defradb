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

	"golang.org/x/exp/constraints"
)

type baseArrayNormalValue[T any] struct {
	NormalVoid
	val  T
	kind FieldKind
}

func (v baseArrayNormalValue[T]) Unwrap() any {
	return v.val
}

func (v baseArrayNormalValue[T]) Kind() FieldKind {
	return v.kind
}

func (v baseArrayNormalValue[T]) IsArray() bool {
	return true
}

func newBaseArrayNormalValue[T any](val T, kind FieldKind) baseArrayNormalValue[T] {
	return baseArrayNormalValue[T]{val: val, kind: kind}
}

type normalBoolArray struct {
	baseArrayNormalValue[[]bool]
}

func (v normalBoolArray) BoolArray() ([]bool, bool) {
	return v.val, true
}

type normalIntArray struct {
	baseArrayNormalValue[[]int64]
}

func (v normalIntArray) IntArray() ([]int64, bool) {
	return v.val, true
}

type normalFloatArray struct {
	baseArrayNormalValue[[]float64]
}

func (v normalFloatArray) FloatArray() ([]float64, bool) {
	return v.val, true
}

type normalStringArray struct {
	baseArrayNormalValue[[]string]
}

func (v normalStringArray) StringArray() ([]string, bool) {
	return v.val, true
}

type normalBytesArray struct {
	baseArrayNormalValue[[][]byte]
}

func (v normalBytesArray) BytesArray() ([][]byte, bool) {
	return v.val, true
}

type normalTimeArray struct {
	baseArrayNormalValue[[]time.Time]
}

func (v normalTimeArray) TimeArray() ([]time.Time, bool) {
	return v.val, true
}

type normalDocumentArray struct {
	baseArrayNormalValue[[]*Document]
}

func (v normalDocumentArray) DocumentArray() ([]*Document, bool) {
	return v.val, true
}

// NewNormalBoolArray creates a new NormalValue that represents a `[]bool` value.
func NewNormalBoolArray(val []bool) NormalValue {
	return normalBoolArray{newBaseArrayNormalValue(val, FieldKind_BOOL_ARRAY)}
}

// NewNormalIntArray creates a new NormalValue that represents a `[]int64` value.
func NewNormalIntArray[T constraints.Integer | constraints.Float](val []T) NormalValue {
	return normalIntArray{newBaseArrayNormalValue(normalizeNumArr[int64](val), FieldKind_INT_ARRAY)}
}

// NewNormalFloatArray creates a new NormalValue that represents a `[]float64` value.
func NewNormalFloatArray[T constraints.Integer | constraints.Float](val []T) NormalValue {
	return normalFloatArray{newBaseArrayNormalValue(normalizeNumArr[float64](val), FieldKind_FLOAT_ARRAY)}
}

// NewNormalStringArray creates a new NormalValue that represents a `[]string` value.
func NewNormalStringArray[T string | []byte](val []T) NormalValue {
	return normalStringArray{newBaseArrayNormalValue(normalizeCharsArr[string](val), FieldKind_STRING_ARRAY)}
}

// NewNormalJSONArray creates a new NormalValue that represents JSON array as a `[]string` value.
func NewNormalJSONArray[T string | []byte](val []T) NormalValue {
	return normalStringArray{newBaseArrayNormalValue(normalizeCharsArr[string](val), FieldKind_JSON_ARRAY)}
}

// NewNormalBytesArray creates a new NormalValue that represents a `[][]byte` value.
func NewNormalBytesArray[T string | []byte](val []T) NormalValue {
	return normalBytesArray{newBaseArrayNormalValue(normalizeCharsArr[[]byte](val), FieldKind_BLOB_ARRAY)}
}

// NewNormalTimeArray creates a new NormalValue that represents a `[]time.Time` value.
func NewNormalTimeArray(val []time.Time) NormalValue {
	return normalTimeArray{newBaseArrayNormalValue(val, FieldKind_DATETIME_ARRAY)}
}

// NewNormalDocumentArray creates a new NormalValue that represents a `[]*Document` value.
func NewNormalDocumentArray(val []*Document, kind ObjectArrayKind) NormalValue {
	return normalDocumentArray{newBaseArrayNormalValue(val, kind)}
}

func normalizeNumArr[R int64 | float64, T constraints.Integer | constraints.Float](val []T) []R {
	var v any = val
	if arr, ok := v.([]R); ok {
		return arr
	}
	arr := make([]R, len(val))
	for i, v := range val {
		arr[i] = R(v)
	}
	return arr
}

func normalizeCharsArr[R string | []byte, T string | []byte](val []T) []R {
	var v any = val
	if arr, ok := v.([]R); ok {
		return arr
	}
	arr := make([]R, len(val))
	for i, v := range val {
		arr[i] = R(v)
	}
	return arr
}
