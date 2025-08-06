// Copyright (c) 2025 ADBC Drivers Contributors
//
// This file has been modified from its original version, which is
// under the Apache License:
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package sqlwrapper

import (
	"database/sql"

	"github.com/apache/arrow-go/v18/arrow"
)

// extractArrowValue extracts a value from an Arrow array at the given index using a TypeConverter
func extractArrowValue(arr arrow.Array, index int, typeConverter TypeConverter) (any, error) {
	return typeConverter.ConvertArrowToGo(arr, index)
}

// buildArrowSchemaFromColumnTypes creates an Arrow schema from SQL column types using the type converter
func buildArrowSchemaFromColumnTypes(columnTypes []*sql.ColumnType, typeConverter TypeConverter) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(columnTypes))
	for i, colType := range columnTypes {
		arrowType, nullable, metadata, err := typeConverter.ConvertColumnType(colType)
		if err != nil {
			return nil, err
		}
		fields[i] = arrow.Field{
			Name:     colType.Name(),
			Type:     arrowType,
			Nullable: nullable,
			Metadata: metadata,
		}
	}
	return arrow.NewSchema(fields, nil), nil
}
