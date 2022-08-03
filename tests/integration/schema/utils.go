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
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/sourcenetwork/defradb/client"
	testutils "github.com/sourcenetwork/defradb/tests/integration"
	"github.com/stretchr/testify/assert"
)

type QueryTestCase struct {
	// The set of schema to add to the database.
	//
	// Each string may contain multiple schema - one string = one db.AddSchema call.
	Schema []string

	// The introspection query to use when fetching schema state.
	//
	// Available properties can be found in the GQL spec:
	// https://spec.graphql.org/October2021/#sec-Introspection
	IntrospectionQuery string

	// The data expected to be returned from the introspection query.
	ExpectedData map[string]interface{}

	// If [ExpectedData] is nil and this is populated, the test framework will assert
	// that the value given exists in the actual results.
	//
	// If this contains nested maps it only requires the last (i.e. non-map) value to
	// be present along the given path.  If an array/slice is present in this chain,
	// it will assert that the items in the expected-array have exact matches in the
	// corresponding result-array (inner maps are not traversed beyond the array,
	// the full array-item must match exactly).
	ContainsData map[string]interface{}

	// Any error expected to be returned by database calls.
	//
	// This includes AddSchema and Introspection calls.
	ExpectedError string
}

type dbInfo interface {
	DB() client.DB
}

func ExecuteQueryTestCase(
	t *testing.T,
	testCase QueryTestCase,
) {
	var err error
	ctx := context.Background()

	var dbi dbInfo
	dbi, err = testutils.NewBadgerMemoryDB(ctx)
	if err != nil {
		t.Fatal(err)
	}

	db := dbi.DB()

	for _, schema := range testCase.Schema {
		err = db.AddSchema(ctx, schema)
		if assertError(t, err, testCase.ExpectedError) {
			return
		}
	}

	result := db.ExecQuery(ctx, testCase.IntrospectionQuery)

	assertSchemaResults(ctx, t, result, testCase)

	if testCase.ExpectedError != "" {
		assert.Fail(t, "Expected an error however none was raised.")
	}
}

func assertSchemaResults(
	ctx context.Context,
	t *testing.T,
	result *client.QueryResult,
	testCase QueryTestCase,
) bool {
	if assertErrors(t, result.Errors, testCase.ExpectedError) {
		return true
	}
	resultantData := result.Data.(map[string]interface{})

	if len(testCase.ExpectedData) == 0 && len(testCase.ContainsData) == 0 {
		assert.Equal(t, testCase.ExpectedData, resultantData)
	}

	if len(testCase.ExpectedData) == 0 && len(testCase.ContainsData) > 0 {
		assertContains(t, testCase.ContainsData, resultantData)
	} else {
		assert.Equal(t, len(testCase.ExpectedData), len(resultantData))

		for k, result := range resultantData {
			assert.Equal(t, testCase.ExpectedData[k], result)
		}
	}

	return false
}

// Asserts that the `actual` contains the given `contains` value according to the logic
// described on the [QueryTestCase.ContainsData] property.
func assertContains(t *testing.T, contains map[string]interface{}, actual map[string]interface{}) {
	for k, expected := range contains {
		innerActual := actual[k]
		if innerExpected, innerIsMap := expected.(map[string]interface{}); innerIsMap {
			if innerActual == nil {
				assert.Equal(t, innerExpected, innerActual)
			} else if innerActualMap, isMap := innerActual.(map[string]interface{}); isMap {
				// If the inner is another map then we continue down the chain
				assertContains(t, innerExpected, innerActualMap)
			} else {
				// If the types don't match then we use assert.Equal for a clean failure message
				assert.Equal(t, innerExpected, innerActual)
			}
		} else if innerExpected, innerIsArray := expected.([]interface{}); innerIsArray {
			if actualArray, isActualArray := innerActual.([]interface{}); isActualArray {
				// If the inner is an array/slice, then assert that each expected item is present
				// in the actual.  Note how the actual may contain additional items - this should
				// not result in a test failure.
				for _, innerExpectedItem := range innerExpected {
					assert.Contains(t, actualArray, innerExpectedItem)
				}
			} else {
				// If the types don't match then we use assert.Equal for a clean failure message
				assert.Equal(t, expected, innerActual)
			}
		} else {
			assert.Equal(t, expected, innerActual)
		}
	}
}

func assertError(t *testing.T, err error, expectedError string) bool {
	if err == nil {
		return false
	}

	if expectedError == "" {
		assert.NoError(t, err)
		return false
	} else {
		if !strings.Contains(err.Error(), expectedError) {
			assert.ErrorIs(t, err, fmt.Errorf(expectedError))
			return false
		}
		return true
	}
}

func assertErrors(
	t *testing.T,
	errors []interface{},
	expectedError string,
) bool {
	if expectedError == "" {
		assert.Empty(t, errors)
	} else {
		for _, e := range errors {
			// This is always a string at the moment, add support for other types as and when needed
			errorString := e.(string)
			if !strings.Contains(errorString, expectedError) {
				// We use ErrorIs for clearer failures (is a error comparision even if it is just a string)
				assert.ErrorIs(t, fmt.Errorf(errorString), fmt.Errorf(expectedError))
				continue
			}
			return true
		}
	}
	return false
}