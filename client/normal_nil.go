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
)

// NewNormalNil creates a new NormalValue that represents a nil value of a given field kind.
func NewNormalNil(kind FieldKind) (NormalValue, error) {
	if kind.IsObjectArray() {
		if k, ok := kind.(ObjectArrayKind); ok && kind.IsNillable() {
			return NewNormalNillableDocumentNillableArray(immutable.None[[]immutable.Option[*Document]](), k), nil
		}
		return nil, NewCanNotMakeNormalNilFromFieldKind(kind)
	}
	if kind.IsObject() {
		if k, ok := kind.(ObjectKind); ok && kind.IsNillable() {
			return NewNormalNillableDocument(immutable.None[*Document](), k), nil
		}
		return nil, NewCanNotMakeNormalNilFromFieldKind(kind)
	}
	switch kind {
	case FieldKind_NILLABLE_BOOL:
		return NewNormalNillableBool(immutable.None[bool]()), nil
	case FieldKind_NILLABLE_INT:
		return NewNormalNillableInt(immutable.None[int64]()), nil
	case FieldKind_NILLABLE_FLOAT:
		return NewNormalNillableFloat(immutable.None[float64]()), nil
	case FieldKind_NILLABLE_DATETIME:
		return NewNormalNillableTime(immutable.None[time.Time]()), nil
	case FieldKind_NILLABLE_STRING:
		return NewNormalNillableString(immutable.None[string]()), nil
	case FieldKind_NILLABLE_JSON:
		return NewNormalNillableJSON(immutable.None[string]()), nil
	case FieldKind_NILLABLE_BLOB:
		return NewNormalNillableBytes(immutable.None[[]byte]()), nil
	case FieldKind_BOOL_NILLABLE_ARRAY:
		return NewNormalBoolNillableArray(immutable.None[[]bool]()), nil
	case FieldKind_INT_NILLABLE_ARRAY:
		return NewNormalIntNillableArray(immutable.None[[]int64]()), nil
	case FieldKind_FLOAT_NILLABLE_ARRAY:
		return NewNormalFloatNillableArray(immutable.None[[]float64]()), nil
	case FieldKind_DATETIME_NILLABLE_ARRAY:
		return NewNormalTimeNillableArray(immutable.None[[]time.Time]()), nil
	case FieldKind_STRING_NILLABLE_ARRAY:
		return NewNormalStringNillableArray(immutable.None[[]string]()), nil
	case FieldKind_JSON_NILLABLE_ARRAY:
		return NewNormalJSONNillableArray(immutable.None[[]string]()), nil
	case FieldKind_BLOB_NILLABLE_ARRAY:
		return NewNormalBytesNillableArray(immutable.None[[][]byte]()), nil
	case FieldKind_NILLABLE_BOOL_NILLABLE_ARRAY:
		return NewNormalNillableBoolNillableArray(immutable.None[[]immutable.Option[bool]]()), nil
	case FieldKind_NILLABLE_INT_NILLABLE_ARRAY:
		return NewNormalNillableIntNillableArray(immutable.None[[]immutable.Option[int64]]()), nil
	case FieldKind_NILLABLE_FLOAT_NILLABLE_ARRAY:
		return NewNormalNillableFloatNillableArray(immutable.None[[]immutable.Option[float64]]()), nil
	case FieldKind_NILLABLE_DATETIME_NILLABLE_ARRAY:
		return NewNormalNillableTimeNillableArray(immutable.None[[]immutable.Option[time.Time]]()), nil
	case FieldKind_NILLABLE_STRING_NILLABLE_ARRAY:
		return NewNormalNillableStringNillableArray(immutable.None[[]immutable.Option[string]]()), nil
	case FieldKind_NILLABLE_JSON_NILLABLE_ARRAY:
		return NewNormalNillableJSONNillableArray(immutable.None[[]immutable.Option[string]]()), nil
	case FieldKind_NILLABLE_BLOB_NILLABLE_ARRAY:
		return NewNormalNillableBytesNillableArray(immutable.None[[]immutable.Option[[]byte]]()), nil
	}
	return nil, NewCanNotMakeNormalNilFromFieldKind(kind)
}
