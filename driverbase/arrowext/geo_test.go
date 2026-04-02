// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package arrowext_test

import (
	"testing"

	"github.com/adbc-drivers/driverbase-go/driverbase/arrowext"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
)

func TestGetGeoArrowSrid(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		input    string
		expected int
		ok       bool
	}{
		{
			name:     "empty",
			input:    ``,
			expected: 0,
			ok:       false,
		},
		{
			name:     "invalid json",
			input:    `{`,
			expected: 0,
			ok:       false,
		},
		{
			name:     "string format 4326",
			input:    `{"crs":"EPSG:4326"}`,
			expected: 4326,
			ok:       true,
		},
		{
			name:     "string format 3857",
			input:    `{"crs":"EPSG:3857"}`,
			expected: 3857,
			ok:       true,
		},
		{
			name:     "string format missing EPSG",
			input:    `{"crs":"WGS84"}`,
			expected: 0,
			ok:       false,
		},
		{
			name:     "projjson like format 4326",
			input:    `{"crs":{"id":{"authority":"EPSG","code":4326}}}`,
			expected: 4326,
			ok:       true,
		},
		{
			name:     "projjson like format string code",
			input:    `{"crs":{"id":{"authority":"EPSG","code":"3857"}}}`,
			expected: 3857,
			ok:       true,
		},
		{
			name:     "projjson like format wrong authority",
			input:    `{"crs":{"id":{"authority":"OGC","code":4326}}}`,
			expected: 0,
			ok:       false,
		},
		{
			name:     "projjson like format lowercase epsg",
			input:    `{"crs":{"id":{"authority":"epsg","code":4326}}}`,
			expected: 4326,
			ok:       true,
		},
		{
			name:     "projjson missing code",
			input:    `{"crs":{"id":{"authority":"EPSG"}}}`,
			expected: 0,
			ok:       false,
		},
		{
			name:     "projjson bad nesting",
			input:    `{"crs":{"authority":"EPSG","code":4326}}`,
			expected: 0,
			ok:       false,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			field := &arrow.Field{
				Name:     "test",
				Type:     arrow.BinaryTypes.Binary,
				Nullable: true,
				Metadata: arrow.MetadataFrom(map[string]string{
					"ARROW:extension:name":     "geoarrow.wkb",
					"ARROW:extension:metadata": testCase.input,
				}),
			}
			srid, ok := arrowext.ExtractGeoArrowSrid(field)
			assert.Equal(t, testCase.ok, ok)
			assert.Equal(t, testCase.expected, srid)
		})
	}
}
