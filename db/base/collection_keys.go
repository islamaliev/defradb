// Copyright 2022 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package base

import (
	"fmt"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/core"
)

// MakeDataStoreKeyWithCollectionDescription returns the datastore key for the given collection description.
func MakeDataStoreKeyWithCollectionDescription(col client.CollectionDescription) core.DataStoreKey {
	return core.DataStoreKey{
		CollectionID: col.IDString(),
	}
}

// MakeDataStoreKeyWithCollectionAndDocID returns the datastore key for the given docID and collection description.
func MakeDataStoreKeyWithCollectionAndDocID(
	col client.CollectionDescription,
	docID string,
) core.DataStoreKey {
	return core.DataStoreKey{
		CollectionID: col.IDString(),
		DocID:        docID,
	}
}

func MakePrimaryIndexKeyForCRDT(
	c client.CollectionDescription,
	schema client.SchemaDescription,
	ctype client.CType,
	key core.DataStoreKey,
	fieldName string,
) (core.DataStoreKey, error) {
	switch ctype {
	case client.COMPOSITE:
		return MakeDataStoreKeyWithCollectionDescription(c).WithInstanceInfo(key).WithFieldId(core.COMPOSITE_NAMESPACE), nil
	case client.LWW_REGISTER, client.PN_COUNTER:
		field, ok := c.GetFieldByName(fieldName, &schema)
		if !ok {
			return core.DataStoreKey{}, client.NewErrFieldNotExist(fieldName)
		}

		return MakeDataStoreKeyWithCollectionDescription(c).WithInstanceInfo(key).WithFieldId(fmt.Sprint(field.ID)), nil
	}
	return core.DataStoreKey{}, ErrInvalidCrdtType
}
