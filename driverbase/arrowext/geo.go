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

package arrowext

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

// ExtractGeoArrowSridFromMetadata extracts the SRID and edges from GeoArrow
// extension metadata string.  The metadata is a JSON string that may contain
// a "crs" field.
//
// Supported formats:
//   - PROJJSON: {"crs": {"id": {"authority": "EPSG", "code": 4326}}}
//   - Simple string: "EPSG:4326" (as CRS value)
//
// Returns 0 if no SRID can be determined.
func ExtractGeoArrowSridFromMetadata(metadata string) (int, string) {
	if metadata == "" {
		return 0, ""
	}

	type projID struct {
		Authority string `json:"authority"`
		Code      int    `json:"code"`
	}
	type projCRS struct {
		ID projID `json:"id"`
	}

	type projIDString struct {
		Authority string `json:"authority"`
		Code      string `json:"code"`
	}
	type projCRSString struct {
		ID projIDString `json:"id"`
	}

	type geoarrowMeta struct {
		CRS   json.RawMessage `json:"crs"`
		Edges string          `json:"edges"`
	}

	var meta geoarrowMeta
	if err := json.Unmarshal([]byte(metadata), &meta); err != nil {
		return 0, ""
	}

	if len(meta.CRS) == 0 {
		return 0, meta.Edges
	}

	// CRS can be a string like "EPSG:4326" or a PROJJSON object
	var crsStr string
	if err := json.Unmarshal(meta.CRS, &crsStr); err == nil {
		if strings.HasPrefix(crsStr, "EPSG:") {
			if code, err := strconv.Atoi(crsStr[5:]); err == nil {
				return code, meta.Edges
			}
		} else if crsStr == "OGC:CRS84" {
			return 4326, meta.Edges
		}
		return 0, meta.Edges
	}

	var crs projCRS
	if err := json.Unmarshal(meta.CRS, &crs); err == nil {
		if strings.EqualFold(crs.ID.Authority, "EPSG") && crs.ID.Code != 0 {
			return crs.ID.Code, meta.Edges
		}
	}

	var crsString projCRSString
	if err := json.Unmarshal(meta.CRS, &crsString); err == nil {
		if strings.EqualFold(crsString.ID.Authority, "EPSG") {
			if code, err := strconv.Atoi(crsString.ID.Code); err == nil {
				return code, meta.Edges
			}
		} else if strings.EqualFold(crsString.ID.Authority, "OGC") && strings.EqualFold(crsString.ID.Code, "CRS84") {
			return 4326, meta.Edges
		}
	}

	return 0, meta.Edges
}

func ExtractGeoArrowSrid(field *arrow.Field) (int, string) {
	ext := GetExtensionName(field)
	if ext != "geoarrow.wkb" && ext != "geoarrow.wkt" {
		return 0, ""
	}

	metadata, mdOk := field.Metadata.GetValue("ARROW:extension:metadata")
	if !mdOk {
		return 0, ""
	}

	return ExtractGeoArrowSridFromMetadata(metadata)
}
