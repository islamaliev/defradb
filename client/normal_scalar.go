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

// NormalValue is dummy implementation of NormalValue to be embedded in other types.
type baseNormalValue[T any] struct {
	NormalVoid
	val  T
	kind FieldKind
}

func (v baseNormalValue[T]) Unwrap() any {
	return v.val
}

func (v baseNormalValue[T]) Kind() FieldKind {
	return v.kind
}

func newBaseNormalValue[T any](val T, kind FieldKind) baseNormalValue[T] {
	return baseNormalValue[T]{val: val, kind: kind}
}

type normalBool struct {
	baseNormalValue[bool]
}

func (v normalBool) Bool() (bool, bool) {
	return v.val, true
}

type normalInt struct {
	baseNormalValue[int64]
}

func (v normalInt) Int() (int64, bool) {
	return v.val, true
}

type normalFloat struct {
	baseNormalValue[float64]
}

func (v normalFloat) Float() (float64, bool) {
	return v.val, true
}

type normalString struct {
	baseNormalValue[string]
}

func (v normalString) String() (string, bool) {
	return v.val, true
}

type normalBytes struct {
	baseNormalValue[[]byte]
}

func (v normalBytes) Bytes() ([]byte, bool) {
	return v.val, true
}

type normalTime struct {
	baseNormalValue[time.Time]
}

func (v normalTime) Time() (time.Time, bool) {
	return v.val, true
}

type normalDocument struct {
	baseNormalValue[*Document]
}

func (v normalDocument) Document() (*Document, bool) {
	return v.val, true
}

// NewNormalBool creates a new NormalValue that represents a `bool` value.
func NewNormalBool(val bool) NormalValue {
	return normalBool{baseNormalValue[bool]{val: val, kind: FieldKind_BOOL}}
}

// NewNormalInt creates a new NormalValue that represents an `int64` value.
func NewNormalInt[T constraints.Integer | constraints.Float](val T) NormalValue {
	return normalInt{newBaseNormalValue(int64(val), FieldKind_INT)}
}

// NewNormalFloat creates a new NormalValue that represents a `float64` value.
func NewNormalFloat[T constraints.Integer | constraints.Float](val T) NormalValue {
	return normalFloat{newBaseNormalValue(float64(val), FieldKind_FLOAT)}
}

// NewNormalString creates a new NormalValue that represents a `string` value.
func NewNormalString[T string | []byte](val T) NormalValue {
	return normalString{baseNormalValue[string]{val: string(val), kind: FieldKind_STRING}}
}

// NewNormalJSON creates a new NormalValue that represents JSON as a `string` value.
func NewNormalJSON[T string | []byte](val T) NormalValue {
	return normalString{baseNormalValue[string]{val: string(val), kind: FieldKind_JSON}}
}

// NewNormalBytes creates a new NormalValue that represents a `[]byte` value.
func NewNormalBytes[T string | []byte](val T) NormalValue {
	return normalBytes{baseNormalValue[[]byte]{val: []byte(val), kind: FieldKind_BLOB}}
}

// NewNormalTime creates a new NormalValue that represents a `time.Time` value.
func NewNormalTime(val time.Time) NormalValue {
	return normalTime{baseNormalValue[time.Time]{val: val, kind: FieldKind_DATETIME}}
}

// NewNormalDocument creates a new NormalValue that represents a `*Document` value.
func NewNormalDocument(val *Document, kind ObjectKind) NormalValue {
	return normalDocument{baseNormalValue[*Document]{val: val, kind: kind}}
}
