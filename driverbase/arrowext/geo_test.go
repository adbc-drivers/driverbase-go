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
	"fmt"
	"testing"

	"github.com/adbc-drivers/driverbase-go/driverbase/arrowext"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
)

func TestGetGeoArrowSrid(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		input string
		srid  int
		edges string
	}{
		{
			name:  "empty",
			input: ``,
			srid:  0,
		},
		{
			name:  "invalid json",
			input: `{`,
			srid:  0,
		},
		{
			name:  "invalid JSON 2",
			input: `not json`,
			srid:  0,
			edges: "",
		},
		{
			name:  "no CRS",
			input: `{"edges":"planar"}`,
			srid:  0,
			edges: "planar",
		},
		{
			name:  "string format 4326",
			input: `{"crs":"EPSG:4326"}`,
			srid:  4326,
		},
		{
			name:  "string format 4326 with edges",
			input: `{"crs":"EPSG:4326", "edges":"spherical"}`,
			srid:  4326,
			edges: "spherical",
		},
		{"string format wgs84", `{"crs":"OGC:CRS84"}`, 4326, ""},
		{
			name:  "string format wgs84 with edges",
			input: `{"crs":"OGC:CRS84", "edges":"spherical"}`,
			srid:  4326,
			edges: "spherical",
		},
		{
			name:  "string format 3857",
			input: `{"crs":"EPSG:3857"}`,
			srid:  3857,
		},
		{
			name:  "string format 3857 with edges",
			input: `{"crs":"EPSG:3857","edges":"spherical"}`,
			srid:  3857,
			edges: "spherical",
		},
		{
			name:  "string format missing EPSG",
			input: `{"crs":"WGS84"}`,
			srid:  0,
		},
		{
			name:  "projjson like format 4326",
			input: `{"crs":{"id":{"authority":"EPSG","code":4326}}}`,
			srid:  4326,
		},
		{
			name:  "projjson like format 4326 with edges",
			input: `{"crs":{"id":{"authority":"EPSG","code":"4326"}},"edges":"spherical"}`,
			srid:  4326,
			edges: "spherical",
		},
		{
			name:  "projjson wgs84",
			input: `{"crs":{"id":{"authority":"OGC","code":"CRS84"}}}`,
			srid:  4326,
		},
		{
			name:  "projjson wgs84 with edges",
			input: `{"crs":{"id":{"authority":"OGC","code":"CRS84"}}, "edges":"spherical"}`,
			srid:  4326,
			edges: "spherical",
		},
		{
			name:  "projjson like format string code",
			input: `{"crs":{"id":{"authority":"EPSG","code":"3857"}}}`,
			srid:  3857,
		},
		{
			name:  "projjson like format wrong authority",
			input: `{"crs":{"id":{"authority":"OGC","code":4326}}}`,
			srid:  0,
		},
		{
			name:  "projjson like format lowercase epsg",
			input: `{"crs":{"id":{"authority":"epsg","code":4326}}}`,
			srid:  4326,
		},
		{
			name:  "projjson missing code",
			input: `{"crs":{"id":{"authority":"EPSG"}}}`,
			srid:  0,
		},
		{
			name:  "projjson bad nesting",
			input: `{"crs":{"authority":"EPSG","code":4326}}`,
			srid:  0,
		},
		{"null CRS", `{"crs":null}`, 0, ""},
	} {
		for _, wkType := range []string{"wkb", "wkt"} {
			name := fmt.Sprintf("%s (%s)", testCase.name, wkType)
			t.Run(name, func(t *testing.T) {
				field := &arrow.Field{
					Name:     "test",
					Type:     arrow.BinaryTypes.Binary,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"ARROW:extension:name":     "geoarrow." + wkType,
						"ARROW:extension:metadata": testCase.input,
					}),
				}
				srid, edges := arrowext.ExtractGeoArrowSrid(field)
				assert.Equal(t, testCase.srid, srid)
				assert.Equal(t, testCase.edges, edges)
			})
		}
	}
}

func TestGetGeoArrowSridInvalid(t *testing.T) {
	t.Run("not geo", func(t *testing.T) {
		field := &arrow.Field{
			Name:     "test",
			Type:     arrow.BinaryTypes.Binary,
			Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{
				"ARROW:extension:name":     "geoarrow.invalid",
				"ARROW:extension:metadata": `{"crs":"EPSG:4326"}`,
			}),
		}
		srid, edges := arrowext.ExtractGeoArrowSrid(field)
		assert.Equal(t, 0, srid)
		assert.Equal(t, "", edges)
	})

	t.Run("no metadata", func(t *testing.T) {
		field := &arrow.Field{
			Name:     "test",
			Type:     arrow.BinaryTypes.Binary,
			Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{
				"ARROW:extension:name": "geoarrow.wkb",
			}),
		}
		srid, edges := arrowext.ExtractGeoArrowSrid(field)
		assert.Equal(t, 0, srid)
		assert.Equal(t, "", edges)
	})
}
