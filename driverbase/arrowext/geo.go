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
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

func ExtractGeoArrowSrid(field *arrow.Field) (int, bool) {
	ext := GetExtensionName(field)
	if ext != "geoarrow.wkb" && ext != "geoarrow.wkt" {
		return 0, false
	}

	metadata, mdOk := field.Metadata.GetValue("ARROW:extension:metadata")
	if !mdOk {
		return 0, false
	} else if metadata == "" {
		return 0, false
	}

	var md map[string]any
	if err := json.Unmarshal([]byte(metadata), &md); err != nil {
		return 0, false
	}
	rawCrs, ok := md["crs"]
	if !ok {
		return 0, false
	}

	if crsStr, ok := rawCrs.(string); ok {
		if !strings.HasPrefix(crsStr, "EPSG:") {
			return 0, false
		}
		var srid int
		if _, err := fmt.Sscanf(crsStr, "EPSG:%d", &srid); err == nil {
			return srid, true
		}
		return 0, false
	}

	if crsObj, ok := rawCrs.(map[string]any); ok {
		idObjRaw, ok := crsObj["id"]
		if !ok {
			return 0, false
		}

		idObj, ok := idObjRaw.(map[string]any)
		if !ok {
			return 0, false
		}

		authRaw, ok := idObj["authority"]
		if !ok {
			return 0, false
		}

		auth, ok := authRaw.(string)
		if !ok || strings.ToUpper(auth) != "EPSG" {
			return 0, false
		}

		codeRaw, ok := idObj["code"]
		if !ok {
			return 0, false
		}

		if code, ok := codeRaw.(float64); ok {
			return int(code), true
		}

		if codeStr, ok := codeRaw.(string); ok {
			if srid, err := strconv.Atoi(codeStr); err == nil {
				return srid, true
			}
		}
	}

	return 0, false
}
