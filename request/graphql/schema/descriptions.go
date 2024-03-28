// Copyright 2022 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schema

import (
	gql "github.com/sourcenetwork/graphql-go"

	"github.com/sourcenetwork/defradb/client"
	schemaTypes "github.com/sourcenetwork/defradb/request/graphql/schema/types"
)

var (
	fieldKindToGQLType = map[client.FieldKind]gql.Type{
		client.FieldKind_DocID:                            gql.ID,
		client.FieldKind_BOOL:                             gql.NewNonNull(gql.Boolean),
		client.FieldKind_INT:                              gql.NewNonNull(gql.Int),
		client.FieldKind_FLOAT:                            gql.NewNonNull(gql.Float),
		client.FieldKind_DATETIME:                         gql.NewNonNull(gql.DateTime),
		client.FieldKind_STRING:                           gql.NewNonNull(gql.String),
		client.FieldKind_BLOB:                             gql.NewNonNull(schemaTypes.BlobScalarType),
		client.FieldKind_JSON:                             gql.NewNonNull(schemaTypes.JSONScalarType),
		client.FieldKind_NILLABLE_BOOL:                    gql.Boolean,
		client.FieldKind_NILLABLE_INT:                     gql.Int,
		client.FieldKind_NILLABLE_FLOAT:                   gql.Float,
		client.FieldKind_NILLABLE_DATETIME:                gql.DateTime,
		client.FieldKind_NILLABLE_STRING:                  gql.String,
		client.FieldKind_NILLABLE_BLOB:                    schemaTypes.BlobScalarType,
		client.FieldKind_NILLABLE_JSON:                    schemaTypes.JSONScalarType,
		client.FieldKind_BOOL_ARRAY:                       gql.NewNonNull(gql.NewList(gql.NewNonNull(gql.Boolean))),
		client.FieldKind_INT_ARRAY:                        gql.NewNonNull(gql.NewList(gql.NewNonNull(gql.Int))),
		client.FieldKind_FLOAT_ARRAY:                      gql.NewNonNull(gql.NewList(gql.NewNonNull(gql.Float))),
		client.FieldKind_DATETIME_ARRAY:                   gql.NewNonNull(gql.NewList(gql.NewNonNull(gql.DateTime))),
		client.FieldKind_STRING_ARRAY:                     gql.NewNonNull(gql.NewList(gql.NewNonNull(gql.String))),
		client.FieldKind_BLOB_ARRAY:                       gql.NewNonNull(gql.NewList(gql.NewNonNull(schemaTypes.BlobScalarType))),
		client.FieldKind_JSON_ARRAY:                       gql.NewNonNull(gql.NewList(gql.NewNonNull(schemaTypes.JSONScalarType))),
		client.FieldKind_BOOL_NILLABLE_ARRAY:              gql.NewList(gql.NewNonNull(gql.Boolean)),
		client.FieldKind_INT_NILLABLE_ARRAY:               gql.NewList(gql.NewNonNull(gql.Int)),
		client.FieldKind_FLOAT_NILLABLE_ARRAY:             gql.NewList(gql.NewNonNull(gql.Float)),
		client.FieldKind_DATETIME_NILLABLE_ARRAY:          gql.NewList(gql.NewNonNull(gql.DateTime)),
		client.FieldKind_STRING_NILLABLE_ARRAY:            gql.NewList(gql.NewNonNull(gql.String)),
		client.FieldKind_BLOB_NILLABLE_ARRAY:              gql.NewList(gql.NewNonNull(schemaTypes.BlobScalarType)),
		client.FieldKind_JSON_NILLABLE_ARRAY:              gql.NewList(gql.NewNonNull(schemaTypes.JSONScalarType)),
		client.FieldKind_NILLABLE_BOOL_ARRAY:              gql.NewNonNull(gql.NewList(gql.Boolean)),
		client.FieldKind_NILLABLE_INT_ARRAY:               gql.NewNonNull(gql.NewList(gql.Int)),
		client.FieldKind_NILLABLE_FLOAT_ARRAY:             gql.NewNonNull(gql.NewList(gql.Float)),
		client.FieldKind_NILLABLE_DATETIME_ARRAY:          gql.NewNonNull(gql.NewList(gql.DateTime)),
		client.FieldKind_NILLABLE_STRING_ARRAY:            gql.NewNonNull(gql.NewList(gql.String)),
		client.FieldKind_NILLABLE_BLOB_ARRAY:              gql.NewNonNull(gql.NewList(schemaTypes.BlobScalarType)),
		client.FieldKind_NILLABLE_JSON_ARRAY:              gql.NewNonNull(gql.NewList(schemaTypes.JSONScalarType)),
		client.FieldKind_NILLABLE_BOOL_NILLABLE_ARRAY:     gql.NewList(gql.Boolean),
		client.FieldKind_NILLABLE_INT_NILLABLE_ARRAY:      gql.NewList(gql.Int),
		client.FieldKind_NILLABLE_FLOAT_NILLABLE_ARRAY:    gql.NewList(gql.Float),
		client.FieldKind_NILLABLE_DATETIME_NILLABLE_ARRAY: gql.NewList(gql.DateTime),
		client.FieldKind_NILLABLE_STRING_NILLABLE_ARRAY:   gql.NewList(gql.String),
		client.FieldKind_NILLABLE_BLOB_NILLABLE_ARRAY:     gql.NewList(schemaTypes.BlobScalarType),
		client.FieldKind_NILLABLE_JSON_NILLABLE_ARRAY:     gql.NewList(schemaTypes.JSONScalarType),
	}

	defaultCRDTForFieldKind = map[client.FieldKind]client.CType{
		client.FieldKind_DocID:                            client.LWW_REGISTER,
		client.FieldKind_BOOL:                             client.LWW_REGISTER,
		client.FieldKind_INT:                              client.LWW_REGISTER,
		client.FieldKind_FLOAT:                            client.LWW_REGISTER,
		client.FieldKind_DATETIME:                         client.LWW_REGISTER,
		client.FieldKind_STRING:                           client.LWW_REGISTER,
		client.FieldKind_BLOB:                             client.LWW_REGISTER,
		client.FieldKind_JSON:                             client.LWW_REGISTER,
		client.FieldKind_NILLABLE_BOOL:                    client.LWW_REGISTER,
		client.FieldKind_NILLABLE_INT:                     client.LWW_REGISTER,
		client.FieldKind_NILLABLE_FLOAT:                   client.LWW_REGISTER,
		client.FieldKind_NILLABLE_DATETIME:                client.LWW_REGISTER,
		client.FieldKind_NILLABLE_STRING:                  client.LWW_REGISTER,
		client.FieldKind_NILLABLE_BLOB:                    client.LWW_REGISTER,
		client.FieldKind_NILLABLE_JSON:                    client.LWW_REGISTER,
		client.FieldKind_BOOL_ARRAY:                       client.LWW_REGISTER,
		client.FieldKind_INT_ARRAY:                        client.LWW_REGISTER,
		client.FieldKind_FLOAT_ARRAY:                      client.LWW_REGISTER,
		client.FieldKind_DATETIME_ARRAY:                   client.LWW_REGISTER,
		client.FieldKind_STRING_ARRAY:                     client.LWW_REGISTER,
		client.FieldKind_BLOB_ARRAY:                       client.LWW_REGISTER,
		client.FieldKind_JSON_ARRAY:                       client.LWW_REGISTER,
		client.FieldKind_BOOL_NILLABLE_ARRAY:              client.LWW_REGISTER,
		client.FieldKind_INT_NILLABLE_ARRAY:               client.LWW_REGISTER,
		client.FieldKind_FLOAT_NILLABLE_ARRAY:             client.LWW_REGISTER,
		client.FieldKind_STRING_NILLABLE_ARRAY:            client.LWW_REGISTER,
		client.FieldKind_DATETIME_NILLABLE_ARRAY:          client.LWW_REGISTER,
		client.FieldKind_BLOB_NILLABLE_ARRAY:              client.LWW_REGISTER,
		client.FieldKind_JSON_NILLABLE_ARRAY:              client.LWW_REGISTER,
		client.FieldKind_NILLABLE_BOOL_ARRAY:              client.LWW_REGISTER,
		client.FieldKind_NILLABLE_INT_ARRAY:               client.LWW_REGISTER,
		client.FieldKind_NILLABLE_FLOAT_ARRAY:             client.LWW_REGISTER,
		client.FieldKind_NILLABLE_STRING_ARRAY:            client.LWW_REGISTER,
		client.FieldKind_NILLABLE_DATETIME_ARRAY:          client.LWW_REGISTER,
		client.FieldKind_NILLABLE_BLOB_ARRAY:              client.LWW_REGISTER,
		client.FieldKind_NILLABLE_JSON_ARRAY:              client.LWW_REGISTER,
		client.FieldKind_NILLABLE_BOOL_NILLABLE_ARRAY:     client.LWW_REGISTER,
		client.FieldKind_NILLABLE_INT_NILLABLE_ARRAY:      client.LWW_REGISTER,
		client.FieldKind_NILLABLE_FLOAT_NILLABLE_ARRAY:    client.LWW_REGISTER,
		client.FieldKind_NILLABLE_STRING_NILLABLE_ARRAY:   client.LWW_REGISTER,
		client.FieldKind_NILLABLE_DATETIME_NILLABLE_ARRAY: client.LWW_REGISTER,
		client.FieldKind_NILLABLE_BLOB_NILLABLE_ARRAY:     client.LWW_REGISTER,
		client.FieldKind_NILLABLE_JSON_NILLABLE_ARRAY:     client.LWW_REGISTER,
	}
)

const (
	docIDFieldDescription string = `
The immutable identifier/docID (primary key) value for this document.
`
	docIDArgDescription string = `
An optional docID parameter for this field. Only documents with
 the given docID will be returned.  If no documents match, the result
 will be null/empty.
`
	docIDsArgDescription string = `
An optional set of docIDs for this field. Only documents with a docID
 matching a docID in the given set will be returned.  If no documents match,
 the result will be null/empty. If an empty set is provided, this argument will
 be ignored.
`
	cidArgDescription string = `
An optional value that specifies the commit ID of the document to return.
 This CID does not need to be the most recent for a document, if it
 corresponds to an older version of a document the document will be returned
 at the state it was in at the time of that commit. If a matching commit is
 not found then an empty set will be returned.
`
	singleFieldFilterArgDescription string = `
An optional filter for this join, if the related record does
 not meet the filter criteria the host record will still be returned,
 but the value of this field will be null.
`
	listFieldFilterArgDescription string = `
An optional filter for this join, if none of the related records meet the filter
 criteria the host record will still be returned, but the value of this field will
 be empty.
`
	selectFilterArgDescription string = `
An optional filter for this select, only documents matching the given criteria
 will be returned.
`
	aggregateFilterArgDescription string = `
An optional filter for this aggregate, only documents matching the given criteria
 will be aggregated.
`
	showDeletedArgDescription string = `
An optional value that specifies as to whether deleted documents may be
 returned. This argument will propagate down through any child selects/joins.
`
	createDocumentDescription string = `
Creates a single document of this type using the data provided.
`
	updateDocumentsDescription string = `
Updates documents in this collection using the data provided. Only documents
 matching any provided criteria will be updated, if no criteria are provided
 the update will be applied to all documents in the collection.
`
	updateIDArgDescription string = `
An optional docID value that will limit the update to the document with
 a matching docID. If no matching document is found, the operation will
 succeed, but no documents will be updated.
`
	updateIDsArgDescription string = `
An optional set of docID values that will limit the update to documents
 with a matching docID. If no matching documents are found, the operation will
 succeed, but no documents will be updated.
`
	updateFilterArgDescription string = `
An optional filter for this update that will limit the update to the documents
 matching the given criteria. If no matching documents are found, the operation
 will succeed, but no documents will be updated.
`
	deleteDocumentsDescription string = `
Deletes documents in this collection matching any provided criteria. If no
 criteria are provided all documents in the collection will be deleted.
`
	deleteIDArgDescription string = `
An optional docID value that will limit the delete to the document with
 a matching docID. If no matching document is found, the operation will
 succeed, but no documents will be deleted.
`
	deleteIDsArgDescription string = `
An optional set of docID values that will limit the delete to documents with
 a matching docID. If no matching documents are found, the operation will
 succeed, but no documents will be deleted. If an empty set is provided, no
 documents will be deleted.
`
	deleteFilterArgDescription string = `
An optional filter for this delete that will limit the delete to documents
 matching the given criteria. If no matching documents are found, the operation
 will succeed, but no documents will be deleted.
`
	groupFieldDescription string = `
The group field may be used to return a set of records belonging to the group.
 It must be used alongside a 'groupBy' argument on the parent selector. It may
 contain any field on the type being grouped, including those used by the
 groupBy.
`
	deletedFieldDescription string = `
Indicates as to whether or not this document has been deleted.
`
	versionFieldDescription string = `
Returns the head commit for this document.
`
)
