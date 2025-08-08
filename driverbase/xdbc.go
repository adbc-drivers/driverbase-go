// Copyright (c) 2025 ADBC Drivers Contributors
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

package driverbase

import "database/sql"

const (
	XdbcDataTypeArray                 int16 = 2003
	XdbcDataTypeBigint                int16 = -5
	XdbcDataTypeBinary                int16 = -2
	XdbcDataTypeBit                   int16 = -7
	XdbcDataTypeBlob                  int16 = 2004
	XdbcDataTypeBoolean               int16 = 16
	XdbcDataTypeChar                  int16 = 1
	XdbcDataTypeClob                  int16 = 2005
	XdbcDataTypeDatalink              int16 = 70
	XdbcDataTypeDate                  int16 = 91
	XdbcDataTypeDecimal               int16 = 3
	XdbcDataTypeDistinct              int16 = 2001
	XdbcDataTypeDouble                int16 = 8
	XdbcDataTypeFloat                 int16 = 6
	XdbcDataTypeInteger               int16 = 4
	XdbcDataTypeJavaObject            int16 = 2000
	XdbcDataTypeLongNVarChar          int16 = -16
	XdbcDataTypeLongVarBinary         int16 = -4
	XdbcDataTypeLongVarChar           int16 = -1
	XdbcDataTypeNChar                 int16 = -15
	XdbcDataTypeNClob                 int16 = 2011
	XdbcDataTypeNull                  int16 = 0
	XdbcDataTypeNumeric               int16 = 2
	XdbcDataTypeNVarChar              int16 = -9
	XdbcDataTypeOther                 int16 = 1111
	XdbcDataTypeReal                  int16 = 7
	XdbcDataTypeRef                   int16 = 2006
	XdbcDataTypeRefCursor             int16 = 2012
	XdbcDataTypeRowId                 int16 = -8
	XdbcDataTypeSmallint              int16 = 5
	XdbcDataTypeSqlXml                int16 = 2009
	XdbcDataTypeStruct                int16 = 2002
	XdbcDataTypeTime                  int16 = 92
	XdbcDataTypeTimeWithTimezone      int16 = 2013
	XdbcDataTypeTimestamp             int16 = 93
	XdbcDataTypeTimestampWithTimezone int16 = 2014
	XdbcDataTypeTinyint               int16 = -6
	XdbcDataTypeVarBinary             int16 = -3
	XdbcDataTypeVarChar               int16 = 12
)

const (
	XdbcColumnNoNulls         int16 = 0
	XdbcColumnNullable        int16 = 1
	XdbcColumnNullableUnknown int16 = 2
)

// NullStringToPtr converts a sql.NullString to a *string.
func NullStringToPtr(s sql.NullString) *string {
	if !s.Valid {
		return nil
	}
	return &s.String
}

// NullInt16ToPtr converts a sql.NullInt16 to a *int16.
func NullInt16ToPtr(i sql.NullInt16) *int16 {
	if !i.Valid {
		return nil
	}
	return &i.Int16
}

// NullInt32ToPtr converts a sql.NullInt32 to a *int32.
func NullInt32ToPtr(i sql.NullInt32) *int32 {
	if !i.Valid {
		return nil
	}
	return &i.Int32
}

func ToPtr[T any](i T) *T {
	return &i
}
