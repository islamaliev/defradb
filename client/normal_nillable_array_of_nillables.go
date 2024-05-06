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

type normalNillableBoolNillableArray struct {
	baseNillableArrayNormalValue[[]immutable.Option[bool]]
}

func (v normalNillableBoolNillableArray) NillableBoolNillableArray() (
	immutable.Option[[]immutable.Option[bool]], bool,
) {
	return v.val, true
}

type normalNillableIntNillableArray struct {
	baseNillableArrayNormalValue[[]immutable.Option[int64]]
}

func (v normalNillableIntNillableArray) NillableIntNillableArray() (
	immutable.Option[[]immutable.Option[int64]], bool,
) {
	return v.val, true
}

type normalNillableFloatNillableArray struct {
	baseNillableArrayNormalValue[[]immutable.Option[float64]]
}

func (v normalNillableFloatNillableArray) NillableFloatNillableArray() (
	immutable.Option[[]immutable.Option[float64]], bool,
) {
	return v.val, true
}

type normalNillableStringNillableArray struct {
	baseNillableArrayNormalValue[[]immutable.Option[string]]
}

func (v normalNillableStringNillableArray) NillableStringNillableArray() (
	immutable.Option[[]immutable.Option[string]], bool,
) {
	return v.val, true
}

type normalNillableBytesNillableArray struct {
	baseNillableArrayNormalValue[[]immutable.Option[[]byte]]
}

func (v normalNillableBytesNillableArray) NillableBytesNillableArray() (
	immutable.Option[[]immutable.Option[[]byte]], bool,
) {
	return v.val, true
}

type normalNillableTimeNillableArray struct {
	baseNillableArrayNormalValue[[]immutable.Option[time.Time]]
}

func (v normalNillableTimeNillableArray) NillableTimeNillableArray() (
	immutable.Option[[]immutable.Option[time.Time]], bool,
) {
	return v.val, true
}

type normalNillableDocumentNillableArray struct {
	baseNillableArrayNormalValue[[]immutable.Option[*Document]]
}

func (v normalNillableDocumentNillableArray) NillableDocumentNillableArray() (
	immutable.Option[[]immutable.Option[*Document]], bool,
) {
	return v.val, true
}

// NewNormalNillableBoolNillableArray creates a new NormalValue that represents a
// `immutable.Option[[]immutable.Option[bool]]` value.
func NewNormalNillableBoolNillableArray(val immutable.Option[[]immutable.Option[bool]]) NormalValue {
	return normalNillableBoolNillableArray{newBaseNillableArrayNormalValue(val)}
}

// NewNormalNillableIntNillableArray creates a new NormalValue that represents a
// `immutable.Option[[]immutable.Option[int64]]` value.
func NewNormalNillableIntNillableArray[T constraints.Integer | constraints.Float](
	val immutable.Option[[]immutable.Option[T]],
) NormalValue {
	return normalNillableIntNillableArray{
		newBaseNillableArrayNormalValue(normalizeNillableNumNillableArr[int64](val)),
	}
}

// NewNormalNillableFloatNillableArray creates a new NormalValue that represents a
// `immutable.Option[[]immutable.Option[float64]]` value.
func NewNormalNillableFloatNillableArray[T constraints.Integer | constraints.Float](
	val immutable.Option[[]immutable.Option[T]],
) NormalValue {
	return normalNillableFloatNillableArray{
		newBaseNillableArrayNormalValue(normalizeNillableNumNillableArr[float64](val)),
	}
}

// NewNormalNillableStringNillableArray creates a new NormalValue that represents a
// `immutable.Option[[]immutable.Option[string]]` value.
func NewNormalNillableStringNillableArray[T string | []byte](val immutable.Option[[]immutable.Option[T]]) NormalValue {
	return normalNillableStringNillableArray{
		newBaseNillableArrayNormalValue(normalizeNillableCharsNillableArr[string](val)),
	}
}

// NewNormalNillableBytesNillableArray creates a new NormalValue that represents a
// `immutable.Option[[]immutable.Option[[]byte]]` value.
func NewNormalNillableBytesNillableArray[T string | []byte](val immutable.Option[[]immutable.Option[T]]) NormalValue {
	return normalNillableBytesNillableArray{
		newBaseNillableArrayNormalValue(normalizeNillableCharsNillableArr[[]byte](val)),
	}
}

// NewNormalNillableTimeNillableArray creates a new NormalValue that represents a
// `immutable.Option[[]immutable.Option[time.Time]]` value.
func NewNormalNillableTimeNillableArray(val immutable.Option[[]immutable.Option[time.Time]]) NormalValue {
	return normalNillableTimeNillableArray{newBaseNillableArrayNormalValue(val)}
}

// NewNormalNillableDocumentNillableArray creates a new NormalValue that represents a
// `immutable.Option[[]immutable.Option[*Document]]` value.
func NewNormalNillableDocumentNillableArray(val immutable.Option[[]immutable.Option[*Document]]) NormalValue {
	return normalNillableDocumentNillableArray{newBaseNillableArrayNormalValue(val)}
}

func normalizeNillableNumNillableArr[R int64 | float64, T constraints.Integer | constraints.Float](
	val immutable.Option[[]immutable.Option[T]],
) immutable.Option[[]immutable.Option[R]] {
	if val.HasValue() {
		return immutable.Some(normalizeNillableNumArr[R](val.Value()))
	}
	return immutable.None[[]immutable.Option[R]]()
}

func normalizeNillableCharsNillableArr[R string | []byte, T string | []byte](
	val immutable.Option[[]immutable.Option[T]],
) immutable.Option[[]immutable.Option[R]] {
	if val.HasValue() {
		return immutable.Some(normalizeNillableCharsArr[R](val.Value()))
	}
	return immutable.None[[]immutable.Option[R]]()
}
