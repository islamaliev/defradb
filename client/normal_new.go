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

// NewNormalValue creates a new NormalValue from the given value.
// It will normalize all known types that can be converted to normal ones.
// For example, if the given type is `[]int32`, it will be converted to `[]int64`.
// If the given value is of type `[]any` it will go through every element and try to convert it
// the most common type and normalizes it.
// For examples, the following conversions will be made:
//   - `[]any{int32(1), int64(2)}` -> `[]int64{1, 2}`.
//   - `[]any{int32(1), int64(2), float32(1.5)}` -> `[]float64{1.0, 2.0, 1.5}`.
//   - `[]any{int32(1), nil}` -> `[]immutable.Option[int64]{immutable.Some(1), immutable.None[int64]()}`.
//
// This function will not check if the given value is `nil`. To normalize a `nil` value use the
// `NewNormalNil` function.
func NewNormalValue(val any, kind FieldKind) (NormalValue, error) {
	if val == nil {
		return NewNormalNil(kind)
	}
	switch v := val.(type) {
	case bool:
		return newNormalBoolOfKind(v, kind)
	case int8:
		return newNormalNumberOfKind(v, kind)
	case int16:
		return newNormalNumberOfKind(v, kind)
	case int32:
		return newNormalNumberOfKind(v, kind)
	case int64:
		return newNormalNumberOfKind(v, kind)
	case int:
		return newNormalNumberOfKind(v, kind)
	case uint8:
		return newNormalNumberOfKind(v, kind)
	case uint16:
		return newNormalNumberOfKind(v, kind)
	case uint32:
		return newNormalNumberOfKind(v, kind)
	case uint64:
		return newNormalNumberOfKind(v, kind)
	case uint:
		return newNormalNumberOfKind(v, kind)
	case float32:
		return newNormalNumberOfKind(v, kind)
	case float64:
		return newNormalNumberOfKind(v, kind)
	case string:
		return newNormalCharsOfKind(v, kind)
	case []byte:
		return newNormalCharsOfKind(v, kind)
	case time.Time:
		return newNormalTimeOfKind(v, kind)
	case *Document:
		return newNormalDocumentOfKind(v, kind)

	case immutable.Option[bool]:
		if kind == FieldKind_NILLABLE_BOOL {
			return NewNormalNillableBool(v), nil
		}
	case immutable.Option[int8]:
		return newNormalNillableNumberOfKind(v, kind)
	case immutable.Option[int16]:
		return newNormalNillableNumberOfKind(v, kind)
	case immutable.Option[int32]:
		return newNormalNillableNumberOfKind(v, kind)
	case immutable.Option[int64]:
		return newNormalNillableNumberOfKind(v, kind)
	case immutable.Option[int]:
		return newNormalNillableNumberOfKind(v, kind)
	case immutable.Option[uint8]:
		return newNormalNillableNumberOfKind(v, kind)
	case immutable.Option[uint16]:
		return newNormalNillableNumberOfKind(v, kind)
	case immutable.Option[uint32]:
		return newNormalNillableNumberOfKind(v, kind)
	case immutable.Option[uint64]:
		return newNormalNillableNumberOfKind(v, kind)
	case immutable.Option[uint]:
		return newNormalNillableNumberOfKind(v, kind)
	case immutable.Option[float32]:
		return newNormalNillableNumberOfKind(v, kind)
	case immutable.Option[float64]:
		return newNormalNillableNumberOfKind(v, kind)
	case immutable.Option[string]:
		return newNormalNillableCharsOfKind(v, kind)
	case immutable.Option[[]byte]:
		return newNormalNillableCharsOfKind(v, kind)
	case immutable.Option[time.Time]:
		if kind == FieldKind_NILLABLE_DATETIME {
			return NewNormalNillableTime(v), nil
		}
	case immutable.Option[*Document]:
		if kind.IsObject() && kind.IsNillable() && !kind.IsObjectArray() {
			return NewNormalNillableDocument(v, kind.(ObjectKind)), nil
		}

	case []bool:
		return newNormalBoolArrayOfKind(v, kind)
	case []int8:
		return newNormalNumberArrayOfKind(v, kind)
	case []int16:
		return newNormalNumberArrayOfKind(v, kind)
	case []int32:
		return newNormalNumberArrayOfKind(v, kind)
	case []int64:
		return newNormalNumberArrayOfKind(v, kind)
	case []int:
		return newNormalNumberArrayOfKind(v, kind)
	case []uint16:
		return newNormalNumberArrayOfKind(v, kind)
	case []uint32:
		return newNormalNumberArrayOfKind(v, kind)
	case []uint64:
		return newNormalNumberArrayOfKind(v, kind)
	case []uint:
		return newNormalNumberArrayOfKind(v, kind)
	case []float32:
		return newNormalNumberArrayOfKind(v, kind)
	case []float64:
		return newNormalNumberArrayOfKind(v, kind)
	case []string:
		return newNormalCharsArrayOfKind(v, kind)
	case [][]byte:
		return newNormalCharsArrayOfKind(v, kind)
	case []time.Time:
		return newNormalTimeArrayOfKind(v, kind)
	case []*Document:
		return newNormalDocumentArrayOfKind(v, kind)

	case []immutable.Option[bool]:
		if kind == FieldKind_NILLABLE_BOOL_ARRAY {
			return NewNormalNillableBoolArray(v), nil
		}
		if kind == FieldKind_NILLABLE_BOOL_NILLABLE_ARRAY {
			return NewNormalNillableBoolNillableArray(immutable.Some(v)), nil
		}
	case []immutable.Option[int8]:
		return newNormalNillableNumberArrayOfKind(v, kind)
	case []immutable.Option[int16]:
		return newNormalNillableNumberArrayOfKind(v, kind)
	case []immutable.Option[int32]:
		return newNormalNillableNumberArrayOfKind(v, kind)
	case []immutable.Option[int64]:
		return newNormalNillableNumberArrayOfKind(v, kind)
	case []immutable.Option[int]:
		return newNormalNillableNumberArrayOfKind(v, kind)
	case []immutable.Option[uint8]:
		return newNormalNillableNumberArrayOfKind(v, kind)
	case []immutable.Option[uint16]:
		return newNormalNillableNumberArrayOfKind(v, kind)
	case []immutable.Option[uint32]:
		return newNormalNillableNumberArrayOfKind(v, kind)
	case []immutable.Option[uint64]:
		return newNormalNillableNumberArrayOfKind(v, kind)
	case []immutable.Option[uint]:
		return newNormalNillableNumberArrayOfKind(v, kind)
	case []immutable.Option[float32]:
		return newNormalNillableNumberArrayOfKind(v, kind)
	case []immutable.Option[float64]:
		return newNormalNillableNumberArrayOfKind(v, kind)
	case []immutable.Option[string]:
		return newNormalNillableCharsArrayOfKind(v, kind)
	case []immutable.Option[[]byte]:
		return newNormalNillableCharsArrayOfKind(v, kind)
	case []immutable.Option[time.Time]:
		if kind == FieldKind_NILLABLE_DATETIME_ARRAY {
			return NewNormalNillableTimeArray(v), nil
		}
		if kind == FieldKind_NILLABLE_DATETIME_NILLABLE_ARRAY {
			return NewNormalNillableTimeNillableArray(immutable.Some(v)), nil
		}
	case []immutable.Option[*Document]:
		if kind.IsObjectArray() {
			k := kind.(ObjectArrayKind)
			if kind.IsNillable() {
				return NewNormalNillableDocumentNillableArray(immutable.Some(v), k), nil
			}
			return NewNormalNillableDocumentArray(v, k), nil
		}

	case immutable.Option[[]bool]:
		if kind == FieldKind_BOOL_NILLABLE_ARRAY {
			return NewNormalBoolNillableArray(v), nil
		}
		if kind == FieldKind_NILLABLE_BOOL_NILLABLE_ARRAY {
			return NewNormalNillableBoolNillableArray(immutable.Some(toArrayOfNillables(v.Value()))), nil
		}
	case immutable.Option[[]int8]:
		return newNormalNumberNillableArrayOfKind(v, kind)
	case immutable.Option[[]int16]:
		return newNormalNumberNillableArrayOfKind(v, kind)
	case immutable.Option[[]int32]:
		return newNormalNumberNillableArrayOfKind(v, kind)
	case immutable.Option[[]int64]:
		return newNormalNumberNillableArrayOfKind(v, kind)
	case immutable.Option[[]int]:
		return newNormalNumberNillableArrayOfKind(v, kind)
	case immutable.Option[[]uint16]:
		return newNormalNumberNillableArrayOfKind(v, kind)
	case immutable.Option[[]uint32]:
		return newNormalNumberNillableArrayOfKind(v, kind)
	case immutable.Option[[]uint64]:
		return newNormalNumberNillableArrayOfKind(v, kind)
	case immutable.Option[[]uint]:
		return newNormalNumberNillableArrayOfKind(v, kind)
	case immutable.Option[[]float32]:
		return newNormalNumberNillableArrayOfKind(v, kind)
	case immutable.Option[[]float64]:
		return newNormalNumberNillableArrayOfKind(v, kind)
	case immutable.Option[[]string]:
		return newNormalCharsNillableArrayOfKind(v, kind)
	case immutable.Option[[][]byte]:
		return newNormalCharsNillableArrayOfKind(v, kind)
	case immutable.Option[[]time.Time]:
		if kind == FieldKind_DATETIME_NILLABLE_ARRAY {
			return NewNormalTimeNillableArray(v), nil
		}
		if kind == FieldKind_NILLABLE_DATETIME_NILLABLE_ARRAY {
			return NewNormalNillableTimeNillableArray(immutable.Some(toArrayOfNillables(v.Value()))), nil
		}
	case immutable.Option[[]*Document]:
		if kind.IsObjectArray() && kind.IsNillable() {
			return NewNormalDocumentNillableArray(v, kind.(ObjectArrayKind)), nil
			// TODO: there is no way to check if the given value should an array of normal documents
			// or an array of nillable documents. There is no FieldKind_NILLABLE_OBJECT_ARRAY.
		}

	case immutable.Option[[]immutable.Option[bool]]:
		if kind == FieldKind_NILLABLE_BOOL_NILLABLE_ARRAY {
			return NewNormalNillableBoolNillableArray(v), nil
		}
	case immutable.Option[[]immutable.Option[int8]]:
		return newNormalNillableNumberNillableArrayOfKind(v, kind)
	case immutable.Option[[]immutable.Option[int16]]:
		return newNormalNillableNumberNillableArrayOfKind(v, kind)
	case immutable.Option[[]immutable.Option[int32]]:
		return newNormalNillableNumberNillableArrayOfKind(v, kind)
	case immutable.Option[[]immutable.Option[int64]]:
		return newNormalNillableNumberNillableArrayOfKind(v, kind)
	case immutable.Option[[]immutable.Option[int]]:
		return newNormalNillableNumberNillableArrayOfKind(v, kind)
	case immutable.Option[[]immutable.Option[uint8]]:
		return newNormalNillableNumberNillableArrayOfKind(v, kind)
	case immutable.Option[[]immutable.Option[uint16]]:
		return newNormalNillableNumberNillableArrayOfKind(v, kind)
	case immutable.Option[[]immutable.Option[uint32]]:
		return newNormalNillableNumberNillableArrayOfKind(v, kind)
	case immutable.Option[[]immutable.Option[uint64]]:
		return newNormalNillableNumberNillableArrayOfKind(v, kind)
	case immutable.Option[[]immutable.Option[uint]]:
		return newNormalNillableNumberNillableArrayOfKind(v, kind)
	case immutable.Option[[]immutable.Option[float32]]:
		return newNormalNillableNumberNillableArrayOfKind(v, kind)
	case immutable.Option[[]immutable.Option[float64]]:
		return newNormalNillableNumberNillableArrayOfKind(v, kind)
	case immutable.Option[[]immutable.Option[string]]:
		return newNormalNillableCharsNillableArrayOfKind(v, kind)
	case immutable.Option[[]immutable.Option[[]byte]]:
		return newNormalNillableCharsNillableArrayOfKind(v, kind)
	case immutable.Option[[]immutable.Option[time.Time]]:
		if kind == FieldKind_NILLABLE_DATETIME_NILLABLE_ARRAY {
			return NewNormalNillableTimeNillableArray(v), nil
		}
	case immutable.Option[[]immutable.Option[*Document]]:
		if kind.IsObjectArray() && kind.IsNillable() {
			return NewNormalNillableDocumentNillableArray(v, kind.(ObjectArrayKind)), nil
		}

	case []any:
		if len(v) == 0 {
			return nil, NewCanNotNormalizeValue(val)
		}
		if !kind.IsArray() {
			return nil, NewCanNotNormalizeValueOfKind(val, kind)
		}

		switch kind {
		case FieldKind_BOOL_ARRAY:
			return convertAnyArrToTypedArrNormalValue[bool](v, NewNormalBoolArray)
		case FieldKind_INT_ARRAY:
			return convertAnyArrToNumArrNormalValue[int64](v, NewNormalIntArray)
		case FieldKind_FLOAT_ARRAY:
			return convertAnyArrToNumArrNormalValue[float64](v, NewNormalFloatArray)
		case FieldKind_STRING_ARRAY:
			return convertAnyArrToTypedArrNormalValue[string](v, NewNormalStringArray)
		case FieldKind_JSON_ARRAY:
			return convertAnyArrToTypedArrNormalValue[string](v, NewNormalJSONArray)
		case FieldKind_BLOB_ARRAY:
			return convertAnyArrToTypedArrNormalValue[[]byte](v, NewNormalBytesArray)
		case FieldKind_DATETIME_ARRAY:
			return convertAnyArrToTypedArrNormalValue[time.Time](v, NewNormalTimeArray)

		case FieldKind_NILLABLE_BOOL_ARRAY:
			return convertAnyArrToNillableTypedArrNormalValue[bool](v, NewNormalNillableBoolArray)
		case FieldKind_NILLABLE_INT_ARRAY:
			return convertAnyArrToNillableNumArrNormalValue[int64](v, NewNormalNillableIntArray)
		case FieldKind_NILLABLE_FLOAT_ARRAY:
			return convertAnyArrToNillableNumArrNormalValue[float64](v, NewNormalNillableFloatArray)
		case FieldKind_NILLABLE_STRING_ARRAY:
			return convertAnyArrToNillableTypedArrNormalValue[string](v, NewNormalNillableStringArray)
		case FieldKind_NILLABLE_JSON_ARRAY:
			return convertAnyArrToNillableTypedArrNormalValue[string](v, NewNormalNillableJSONArray)
		case FieldKind_NILLABLE_BLOB_ARRAY:
			return convertAnyArrToNillableTypedArrNormalValue[[]byte](v, NewNormalNillableBytesArray)
		case FieldKind_NILLABLE_DATETIME_ARRAY:
			return convertAnyArrToNillableTypedArrNormalValue[time.Time](v, NewNormalNillableTimeArray)

		case FieldKind_BOOL_NILLABLE_ARRAY:
			return convertAnyArrToTypedNillableArrNormalValue(v, NewNormalBoolNillableArray)
		case FieldKind_INT_NILLABLE_ARRAY:
			return convertAnyArrToNumArrNillableNormalValue[int64](v, NewNormalIntNillableArray)
		case FieldKind_FLOAT_NILLABLE_ARRAY:
			return convertAnyArrToNumArrNillableNormalValue[float64](v, NewNormalFloatNillableArray)
		case FieldKind_STRING_NILLABLE_ARRAY:
			return convertAnyArrToTypedNillableArrNormalValue[string](v, NewNormalStringNillableArray)
		case FieldKind_JSON_NILLABLE_ARRAY:
			return convertAnyArrToTypedNillableArrNormalValue[string](v, NewNormalJSONNillableArray)
		case FieldKind_BLOB_NILLABLE_ARRAY:
			return convertAnyArrToTypedNillableArrNormalValue[[]byte](v, NewNormalBytesNillableArray)
		case FieldKind_DATETIME_NILLABLE_ARRAY:
			return convertAnyArrToTypedNillableArrNormalValue[time.Time](v, NewNormalTimeNillableArray)

		case FieldKind_NILLABLE_BOOL_NILLABLE_ARRAY:
			return convertAnyArrToNillableTypedNillableArrNormalValue(v, NewNormalNillableBoolNillableArray)
		case FieldKind_NILLABLE_INT_NILLABLE_ARRAY:
			return convertAnyArrToNillableNumNillableArrNormalValue[int64](v, NewNormalNillableIntNillableArray)
		case FieldKind_NILLABLE_FLOAT_NILLABLE_ARRAY:
			return convertAnyArrToNillableNumNillableArrNormalValue[float64](v, NewNormalNillableIntNillableArray)
		case FieldKind_NILLABLE_STRING_NILLABLE_ARRAY:
			return convertAnyArrToNillableTypedNillableArrNormalValue[string](v, NewNormalNillableStringNillableArray)
		case FieldKind_NILLABLE_JSON_NILLABLE_ARRAY:
			return convertAnyArrToNillableTypedNillableArrNormalValue[string](v, NewNormalNillableJSONNillableArray)
		case FieldKind_NILLABLE_BLOB_NILLABLE_ARRAY:
			return convertAnyArrToNillableTypedNillableArrNormalValue[[]byte](v, NewNormalNillableBytesNillableArray)
		case FieldKind_NILLABLE_DATETIME_NILLABLE_ARRAY:
			return convertAnyArrToNillableTypedNillableArrNormalValue[time.Time](v, NewNormalNillableTimeNillableArray)
		}

		if kind.IsObjectArray() {
			if kind.IsNillable() {
				return convertAnyArrToTypedNillableArrNormalValue(v, func(d immutable.Option[[]*Document]) NormalValue {
					return NewNormalDocumentNillableArray(d, ObjectArrayKind(kind.Underlying()))
				})
			}
			return convertAnyArrToTypedArrNormalValue(v, func(d []*Document) NormalValue {
				return NewNormalDocumentArray(d, ObjectArrayKind(kind.Underlying()))
			})
		}
	}
	return nil, NewCanNotNormalizeValueOfKind(val, kind)
}

func newNormalBoolOfKind(val bool, kind FieldKind) (NormalValue, error) {
	switch kind {
	case FieldKind_BOOL:
		return NewNormalBool(val), nil
	case FieldKind_NILLABLE_BOOL:
		return NewNormalNillableBool(immutable.Some(val)), nil
	}
	return nil, NewCanNotNormalizeValueOfKind(val, kind)
}

func newNormalBoolArrayOfKind(val []bool, kind FieldKind) (NormalValue, error) {
	switch kind {
	case FieldKind_BOOL_ARRAY:
		return NewNormalBoolArray(val), nil
	case FieldKind_BOOL_NILLABLE_ARRAY:
		return NewNormalBoolNillableArray(immutable.Some(val)), nil
	case FieldKind_NILLABLE_BOOL_ARRAY:
		return NewNormalNillableBoolArray(toArrayOfNillables(val)), nil
	case FieldKind_NILLABLE_BOOL_NILLABLE_ARRAY:
		return NewNormalNillableBoolNillableArray(immutable.Some(toArrayOfNillables(val))), nil
	}
	return nil, NewCanNotNormalizeValueOfKind(val, kind)
}

func newNormalNumberOfKind[T constraints.Integer | constraints.Float](val T, kind FieldKind) (NormalValue, error) {
	switch kind {
	case FieldKind_INT:
		return NewNormalInt(val), nil
	case FieldKind_FLOAT:
		return NewNormalFloat(val), nil
	case FieldKind_NILLABLE_INT:
		return NewNormalNillableInt(immutable.Some(int64(val))), nil
	case FieldKind_NILLABLE_FLOAT:
		return NewNormalNillableFloat(immutable.Some(float64(val))), nil
	}
	return nil, NewCanNotNormalizeValueOfKind(val, kind)
}

func newNormalNillableNumberOfKind[T constraints.Integer | constraints.Float](
	val immutable.Option[T],
	kind FieldKind,
) (NormalValue, error) {
	switch kind {
	case FieldKind_NILLABLE_INT:
		return NewNormalNillableInt(val), nil
	case FieldKind_NILLABLE_FLOAT:
		return NewNormalNillableFloat(val), nil
	}
	return nil, NewCanNotNormalizeValueOfKind(val, kind)
}

func newNormalNumberArrayOfKind[T constraints.Integer | constraints.Float](
	val []T, kind FieldKind,
) (NormalValue, error) {
	switch kind {
	case FieldKind_INT_ARRAY:
		return NewNormalIntArray(val), nil
	case FieldKind_FLOAT_ARRAY:
		return NewNormalFloatArray(val), nil
	case FieldKind_INT_NILLABLE_ARRAY:
		return NewNormalIntNillableArray(immutable.Some(val)), nil
	case FieldKind_FLOAT_NILLABLE_ARRAY:
		return NewNormalFloatNillableArray(immutable.Some(val)), nil
	case FieldKind_NILLABLE_FLOAT_ARRAY:
		return NewNormalNillableFloatArray(toArrayOfNillables(val)), nil
	case FieldKind_NILLABLE_INT_ARRAY:
		return NewNormalNillableIntArray(toArrayOfNillables(val)), nil
	case FieldKind_NILLABLE_INT_NILLABLE_ARRAY:
		return NewNormalNillableIntNillableArray(immutable.Some(toArrayOfNillables(val))), nil
	case FieldKind_NILLABLE_FLOAT_NILLABLE_ARRAY:
		return NewNormalNillableFloatNillableArray(immutable.Some(toArrayOfNillables(val))), nil
	}
	return nil, NewCanNotNormalizeValueOfKind(val, kind)
}

func newNormalNillableNumberArrayOfKind[T constraints.Integer | constraints.Float](
	val []immutable.Option[T], kind FieldKind,
) (NormalValue, error) {
	switch kind {
	case FieldKind_NILLABLE_INT_ARRAY:
		return NewNormalNillableIntArray(val), nil
	case FieldKind_NILLABLE_FLOAT_ARRAY:
		return NewNormalNillableFloatArray(val), nil
	case FieldKind_NILLABLE_INT_NILLABLE_ARRAY:
		return NewNormalNillableIntNillableArray(immutable.Some(val)), nil
	case FieldKind_NILLABLE_FLOAT_NILLABLE_ARRAY:
		return NewNormalNillableFloatNillableArray(immutable.Some(val)), nil
	}
	return nil, NewCanNotNormalizeValueOfKind(val, kind)
}

func newNormalNumberNillableArrayOfKind[T constraints.Integer | constraints.Float](
	val immutable.Option[[]T], kind FieldKind,
) (NormalValue, error) {
	switch kind {
	case FieldKind_INT_NILLABLE_ARRAY:
		return NewNormalIntNillableArray(val), nil
	case FieldKind_NILLABLE_INT_NILLABLE_ARRAY:
		return NewNormalNillableIntNillableArray(immutable.Some(toArrayOfNillables(val.Value()))), nil
	case FieldKind_FLOAT_NILLABLE_ARRAY:
		return NewNormalFloatNillableArray(val), nil
	case FieldKind_NILLABLE_FLOAT_NILLABLE_ARRAY:
		return NewNormalNillableFloatNillableArray(immutable.Some(toArrayOfNillables(val.Value()))), nil
	}
	return nil, NewCanNotNormalizeValueOfKind(val, kind)
}

func newNormalNillableNumberNillableArrayOfKind[T constraints.Integer | constraints.Float](
	val immutable.Option[[]immutable.Option[T]], kind FieldKind,
) (NormalValue, error) {
	switch kind {
	case FieldKind_NILLABLE_INT_NILLABLE_ARRAY:
		return NewNormalNillableIntNillableArray(val), nil
	case FieldKind_NILLABLE_FLOAT_NILLABLE_ARRAY:
		return NewNormalNillableFloatNillableArray(val), nil
	}
	return nil, NewCanNotNormalizeValueOfKind(val, kind)
}

func newNormalCharsOfKind[T string | []byte](
	val T, kind FieldKind,
) (NormalValue, error) {
	switch kind {
	case FieldKind_STRING:
		return NewNormalString(string(val)), nil
	case FieldKind_JSON:
		return NewNormalJSON(string(val)), nil
	case FieldKind_BLOB:
		return NewNormalBytes([]byte(val)), nil
	case FieldKind_NILLABLE_STRING:
		return NewNormalNillableString(immutable.Some(string(val))), nil
	case FieldKind_NILLABLE_JSON:
		return NewNormalNillableJSON(immutable.Some(string(val))), nil
	case FieldKind_NILLABLE_BLOB:
		return NewNormalNillableBytes(immutable.Some([]byte(val))), nil
	}
	return nil, NewCanNotNormalizeValueOfKind(val, kind)
}

func newNormalNillableCharsOfKind[T string | []byte](val immutable.Option[T], kind FieldKind) (NormalValue, error) {
	switch kind {
	case FieldKind_NILLABLE_STRING:
		return NewNormalNillableString(val), nil
	case FieldKind_NILLABLE_JSON:
		return NewNormalNillableJSON(val), nil
	case FieldKind_NILLABLE_BLOB:
		return NewNormalNillableBytes(val), nil
	}
	return nil, NewCanNotNormalizeValueOfKind(val, kind)
}

func newNormalCharsArrayOfKind[T string | []byte](
	val []T, kind FieldKind,
) (NormalValue, error) {
	switch kind {
	case FieldKind_STRING_ARRAY:
		return NewNormalStringArray(val), nil
	case FieldKind_JSON_ARRAY:
		return NewNormalJSONArray(val), nil
	case FieldKind_BLOB_ARRAY:
		return NewNormalBytesArray(val), nil
	case FieldKind_STRING_NILLABLE_ARRAY:
		return NewNormalStringNillableArray(immutable.Some(val)), nil
	case FieldKind_JSON_NILLABLE_ARRAY:
		return NewNormalJSONNillableArray(immutable.Some(val)), nil
	case FieldKind_BLOB_NILLABLE_ARRAY:
		return NewNormalBytesNillableArray(immutable.Some(val)), nil
	case FieldKind_NILLABLE_STRING_ARRAY:
		return NewNormalNillableStringArray(toArrayOfNillables(val)), nil
	case FieldKind_NILLABLE_JSON_ARRAY:
		return NewNormalNillableJSONArray(toArrayOfNillables(val)), nil
	case FieldKind_NILLABLE_BLOB_ARRAY:
		return NewNormalNillableBytesArray(toArrayOfNillables(val)), nil
	case FieldKind_NILLABLE_STRING_NILLABLE_ARRAY:
		return NewNormalNillableStringNillableArray(immutable.Some(toArrayOfNillables(val))), nil
	case FieldKind_NILLABLE_JSON_NILLABLE_ARRAY:
		return NewNormalNillableJSONNillableArray(immutable.Some(toArrayOfNillables(val))), nil
	case FieldKind_NILLABLE_BLOB_NILLABLE_ARRAY:
		return NewNormalNillableBytesNillableArray(immutable.Some(toArrayOfNillables(val))), nil
	}
	return nil, NewCanNotNormalizeValueOfKind(val, kind)
}

func newNormalNillableCharsArrayOfKind[T string | []byte](
	val []immutable.Option[T], kind FieldKind,
) (NormalValue, error) {
	switch kind {
	case FieldKind_NILLABLE_STRING_ARRAY:
		return NewNormalNillableStringArray(val), nil
	case FieldKind_NILLABLE_JSON_ARRAY:
		return NewNormalNillableJSONArray(val), nil
	case FieldKind_NILLABLE_BLOB_ARRAY:
		return NewNormalNillableBytesArray(val), nil
	case FieldKind_NILLABLE_STRING_NILLABLE_ARRAY:
		return NewNormalNillableStringNillableArray(immutable.Some(val)), nil
	case FieldKind_NILLABLE_JSON_NILLABLE_ARRAY:
		return NewNormalNillableJSONNillableArray(immutable.Some(val)), nil
	case FieldKind_NILLABLE_BLOB_NILLABLE_ARRAY:
		return NewNormalNillableBytesNillableArray(immutable.Some(val)), nil
	}
	return nil, NewCanNotNormalizeValueOfKind(val, kind)
}

func newNormalCharsNillableArrayOfKind[T string | []byte](
	val immutable.Option[[]T], kind FieldKind,
) (NormalValue, error) {
	switch kind {
	case FieldKind_STRING_NILLABLE_ARRAY:
		return NewNormalStringNillableArray(val), nil
	case FieldKind_JSON_NILLABLE_ARRAY:
		return NewNormalJSONNillableArray(val), nil
	case FieldKind_BLOB_NILLABLE_ARRAY:
		return NewNormalBytesNillableArray(val), nil
	case FieldKind_NILLABLE_STRING_NILLABLE_ARRAY:
		return NewNormalNillableStringNillableArray(immutable.Some(toArrayOfNillables(val.Value()))), nil
	case FieldKind_NILLABLE_JSON_NILLABLE_ARRAY:
		return NewNormalNillableJSONNillableArray(immutable.Some(toArrayOfNillables(val.Value()))), nil
	case FieldKind_NILLABLE_BLOB_NILLABLE_ARRAY:
		return NewNormalNillableBytesNillableArray(immutable.Some(toArrayOfNillables(val.Value()))), nil
	}
	return nil, NewCanNotNormalizeValueOfKind(val, kind)
}

func newNormalNillableCharsNillableArrayOfKind[T string | []byte](
	val immutable.Option[[]immutable.Option[T]], kind FieldKind,
) (NormalValue, error) {
	switch kind {
	case FieldKind_NILLABLE_STRING_NILLABLE_ARRAY:
		return NewNormalNillableStringNillableArray(val), nil
	case FieldKind_NILLABLE_JSON_NILLABLE_ARRAY:
		return NewNormalNillableJSONNillableArray(val), nil
	case FieldKind_NILLABLE_BLOB_NILLABLE_ARRAY:
		return NewNormalNillableBytesNillableArray(val), nil
	}
	return nil, NewCanNotNormalizeValueOfKind(val, kind)
}

func newNormalTimeOfKind(val time.Time, kind FieldKind) (NormalValue, error) {
	switch kind {
	case FieldKind_DATETIME:
		return NewNormalTime(val), nil
	case FieldKind_NILLABLE_DATETIME:
		return NewNormalNillableTime(immutable.Some(val)), nil
	}
	return nil, NewCanNotNormalizeValueOfKind(val, kind)
}

func newNormalDocumentOfKind(val *Document, kind FieldKind) (NormalValue, error) {
	if !kind.IsObject() || kind.IsObjectArray() {
		return nil, NewCanNotNormalizeValueOfKind(val, kind)
	}
	if kind.IsNillable() {
		return NewNormalNillableDocument(immutable.Some(val), kind.(ObjectKind)), nil
	}
	return NewNormalDocument(val, kind.(ObjectKind)), nil
}

func newNormalTimeArrayOfKind(val []time.Time, kind FieldKind) (NormalValue, error) {
	switch kind {
	case FieldKind_DATETIME_ARRAY:
		return NewNormalTimeArray(val), nil
	case FieldKind_NILLABLE_DATETIME_ARRAY:
		return NewNormalNillableTimeArray(toArrayOfNillables(val)), nil
	case FieldKind_DATETIME_NILLABLE_ARRAY:
		return NewNormalTimeNillableArray(immutable.Some(val)), nil
	case FieldKind_NILLABLE_DATETIME_NILLABLE_ARRAY:
		return NewNormalNillableTimeNillableArray(immutable.Some(toArrayOfNillables(val))), nil
	}
	return nil, NewCanNotNormalizeValueOfKind(val, kind)
}

func newNormalDocumentArrayOfKind(val []*Document, kind FieldKind) (NormalValue, error) {
	if kind.IsObjectArray() {
		k := kind.(ObjectArrayKind)
		if kind.IsNillable() {
			return NewNormalDocumentNillableArray(immutable.Some(val), k), nil
		}
		return NewNormalDocumentArray(val, k), nil
	}
	return nil, NewCanNotNormalizeValueOfKind(val, kind)
}

func toArrayOfNillables[T any](arr []T) []immutable.Option[T] {
	result := make([]immutable.Option[T], len(arr))
	for i := range arr {
		result[i] = immutable.Some(arr[i])
	}
	return result
}

func convertAnyArrToTypedArrNormalValue[T any](
	arr []any,
	newFunc func([]T) NormalValue,
) (NormalValue, error) {
	result := convertAnyArrToTypedArr[T](arr)
	if result == nil {
		return nil, NewCanNotNormalizeValue(arr)
	}
	return newFunc(result), nil
}

func convertAnyArrToTypedNillableArrNormalValue[T any](
	arr []any,
	newFunc func(immutable.Option[[]T]) NormalValue,
) (NormalValue, error) {
	result := convertAnyArrToTypedArr[T](arr)
	if result == nil {
		return nil, NewCanNotNormalizeValue(arr)
	}
	return newFunc(immutable.Some(result)), nil
}

func convertAnyArrToTypedArr[T any](arr []any) []T {
	result := make([]T, len(arr))
	for i := range arr {
		if v, ok := arr[i].(T); ok {
			result[i] = v
		} else {
			return nil
		}
	}
	return result
}

func convertAnyArrToNillableTypedNillableArrNormalValue[T any](
	arr []any,
	newFunc func(immutable.Option[[]immutable.Option[T]]) NormalValue,
) (NormalValue, error) {
	result := convertAnyArrToNillableTypedArr[T](arr)
	if result == nil {
		return nil, NewCanNotNormalizeValue(arr)
	}
	return newFunc(immutable.Some(result)), nil
}

func convertAnyArrToNillableTypedArrNormalValue[T any](
	arr []any,
	newFunc func([]immutable.Option[T]) NormalValue,
) (NormalValue, error) {
	result := convertAnyArrToNillableTypedArr[T](arr)
	if result == nil {
		return nil, NewCanNotNormalizeValue(arr)
	}
	return newFunc(result), nil
}

func convertAnyArrToNillableTypedArr[T any](
	arr []any,
) []immutable.Option[T] {
	result := make([]immutable.Option[T], len(arr))
	for i := range arr {
		if arr[i] == nil {
			result[i] = immutable.None[T]()
			continue
		}
		if v, ok := arr[i].(T); ok {
			result[i] = immutable.Some(v)
		} else if v, ok := arr[i].(immutable.Option[T]); ok {
			result[i] = v
		} else {
			return nil
		}
	}
	return result
}

func convertAnyArrToNumArrNillableNormalValue[T int64 | float64](
	arr []any,
	newFunc func(immutable.Option[[]T]) NormalValue,
) (NormalValue, error) {
	result := convertAnyArrToNumArr[T](arr)
	if result == nil {
		return nil, NewCanNotNormalizeValue(arr)
	}
	return newFunc(immutable.Some(result)), nil
}

func convertAnyArrToNumArrNormalValue[T int64 | float64](
	arr []any,
	newFunc func([]T) NormalValue,
) (NormalValue, error) {
	result := convertAnyArrToNumArr[T](arr)
	if result == nil {
		return nil, NewCanNotNormalizeValue(arr)
	}
	return newFunc(result), nil
}

func convertAnyArrToNumArr[T int64 | float64](arr []any) []T {
	result := make([]T, len(arr))
	for i := range arr {
		switch v := arr[i].(type) {
		case int8:
			result[i] = T(v)
		case int16:
			result[i] = T(v)
		case int32:
			result[i] = T(v)
		case int64:
			result[i] = T(v)
		case int:
			result[i] = T(v)
		case uint8:
			result[i] = T(v)
		case uint16:
			result[i] = T(v)
		case uint32:
			result[i] = T(v)
		case uint64:
			result[i] = T(v)
		case uint:
			result[i] = T(v)
		case float32:
			result[i] = T(v)
		case float64:
			result[i] = T(v)
		default:
			return nil
		}
	}
	return result
}

func convertAnyArrToNillableNumArrNormalValue[T int64 | float64](
	arr []any,
	newFunc func([]immutable.Option[T]) NormalValue,
) (NormalValue, error) {
	result := convertAnyArrToNillableNumArr[T](arr)
	if result == nil {
		return nil, NewCanNotNormalizeValue(arr)
	}
	return newFunc(result), nil
}

func convertAnyArrToNillableNumNillableArrNormalValue[T int64 | float64](
	arr []any,
	newFunc func(immutable.Option[[]immutable.Option[T]]) NormalValue,
) (NormalValue, error) {
	result := convertAnyArrToNillableNumArr[T](arr)
	if result == nil {
		return nil, NewCanNotNormalizeValue(arr)
	}
	return newFunc(immutable.Some(result)), nil
}

func convertAnyArrToNillableNumArr[T int64 | float64](arr []any) []immutable.Option[T] {
	result := make([]immutable.Option[T], len(arr))
	for i := range arr {
		if arr[i] == nil {
			result[i] = immutable.None[T]()
			continue
		}
		switch v := arr[i].(type) {
		case int8:
			result[i] = immutable.Some(T(v))
		case int16:
			result[i] = immutable.Some(T(v))
		case int32:
			result[i] = immutable.Some(T(v))
		case int64:
			result[i] = immutable.Some(T(v))
		case int:
			result[i] = immutable.Some(T(v))
		case uint8:
			result[i] = immutable.Some(T(v))
		case uint16:
			result[i] = immutable.Some(T(v))
		case uint32:
			result[i] = immutable.Some(T(v))
		case uint64:
			result[i] = immutable.Some(T(v))
		case uint:
			result[i] = immutable.Some(T(v))
		case float32:
			result[i] = immutable.Some(T(v))
		case float64:
			result[i] = immutable.Some(T(v))
		case immutable.Option[int8]:
			result[i] = convertNumOption[T](v)
		case immutable.Option[int16]:
			result[i] = convertNumOption[T](v)
		case immutable.Option[int32]:
			result[i] = convertNumOption[T](v)
		case immutable.Option[int64]:
			result[i] = convertNumOption[T](v)
		case immutable.Option[int]:
			result[i] = convertNumOption[T](v)
		case immutable.Option[uint8]:
			result[i] = convertNumOption[T](v)
		case immutable.Option[uint16]:
			result[i] = convertNumOption[T](v)
		case immutable.Option[uint32]:
			result[i] = convertNumOption[T](v)
		case immutable.Option[uint64]:
			result[i] = convertNumOption[T](v)
		case immutable.Option[uint]:
			result[i] = convertNumOption[T](v)
		case immutable.Option[float32]:
			result[i] = convertNumOption[T](v)
		case immutable.Option[float64]:
			result[i] = convertNumOption[T](v)
		default:
			return nil
		}
	}
	return result
}

func convertNumOption[T int64 | float64, I constraints.Integer | constraints.Float](
	opt immutable.Option[I],
) immutable.Option[T] {
	if opt.HasValue() {
		return immutable.Some(T(opt.Value()))
	}
	return immutable.None[T]()
}
