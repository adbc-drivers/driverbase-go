package mysql

import (
	"database/sql"
	"fmt"
	"strings"

	// register the "mysql" driver with database/sql
	_ "github.com/go-sql-driver/mysql"

	sqlwrapper "github.com/adbc-drivers/driverbase-go/sql"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// MySQLTypeConverter provides MySQL-specific type conversion enhancements
type mySQLTypeConverter struct {
	sqlwrapper.DefaultTypeConverter
}

// ConvertColumnType implements TypeConverter with MySQL-specific enhancements
func (m *mySQLTypeConverter) ConvertColumnType(colType *sql.ColumnType) (arrow.DataType, bool, arrow.Metadata, error) {
	typeName := strings.ToUpper(colType.DatabaseTypeName())

	switch typeName {
	case "JSON":
		// Convert MySQL JSON to Arrow string with special metadata
		metadataMap := map[string]string{
			"sql.database_type_name": colType.DatabaseTypeName(),
			"sql.column_name":        colType.Name(),
			"mysql.is_json":          "true",
		}

		// Add length if available
		if length, ok := colType.Length(); ok {
			metadataMap["sql.length"] = fmt.Sprintf("%d", length)
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.BinaryTypes.String, true, metadata, nil

	case "GEOMETRY", "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON":
		// Convert MySQL spatial types to binary with spatial metadata
		metadata := arrow.MetadataFrom(map[string]string{
			"sql.database_type_name": colType.DatabaseTypeName(),
			"sql.column_name":        colType.Name(),
			"mysql.is_spatial":       "true",
		})
		return arrow.BinaryTypes.Binary, true, metadata, nil

	case "ENUM", "SET":
		// Handle ENUM/SET as string with special metadata
		metadataMap := map[string]string{
			"sql.database_type_name": colType.DatabaseTypeName(),
			"sql.column_name":        colType.Name(),
			"mysql.is_enum_set":      "true",
		}

		if length, ok := colType.Length(); ok {
			metadataMap["sql.length"] = fmt.Sprintf("%d", length)
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.BinaryTypes.String, true, metadata, nil

	default:
		// Fall back to default conversion for standard types
		return m.DefaultTypeConverter.ConvertColumnType(colType)
	}
}

// ConvertSQLToArrow implements MySQL-specific SQL value to Arrow value conversion
func (m *mySQLTypeConverter) ConvertSQLToArrow(sqlValue any, arrowType arrow.DataType) (any, error) {
	// Handle MySQL-specific type conversions
	switch arrowType := arrowType.(type) {
	case *arrow.TimestampType:
		// MySQL TIMESTAMP is timezone-aware, but the generic converter creates timezone-naive types
		// If we detect a TIMESTAMP column type (vs DATETIME), we should handle timezone conversion
		if metadata := getColumnMetadata(arrowType); metadata != nil {
			if dbTypeName, ok := metadata["sql.database_type_name"]; ok && strings.ToUpper(dbTypeName) == "TIMESTAMP" {
				// For MySQL TIMESTAMP columns, ensure timezone handling
				// The sqlValue should be a time.Time already converted to the session timezone
				return sqlValue, nil // Let the default logic handle it, but with timezone awareness
			}
		}
		// For DATETIME, fall through to default behavior (timezone-naive)
		return m.DefaultTypeConverter.ConvertSQLToArrow(sqlValue, arrowType)
		
	case *arrow.StringType:
		// Handle MySQL JSON types specially  
		if metadata := getColumnMetadata(arrowType); metadata != nil {
			if isJSON, ok := metadata["mysql.is_json"]; ok && isJSON == "true" {
				// For JSON columns, we might want to validate or pretty-format JSON
				switch v := sqlValue.(type) {
				case []byte:
					// MySQL returns JSON as []byte, convert to string
					return string(v), nil
				case string:
					return v, nil
				default:
					return fmt.Sprintf("%v", sqlValue), nil
				}
			}
		}
		// Fall through to default for non-JSON strings
		return m.DefaultTypeConverter.ConvertSQLToArrow(sqlValue, arrowType)
		
	case *arrow.BinaryType:
		// Handle MySQL spatial types
		if metadata := getColumnMetadata(arrowType); metadata != nil {
			if isSpatial, ok := metadata["mysql.is_spatial"]; ok && isSpatial == "true" {
				// For spatial types, ensure we preserve binary data correctly
				switch v := sqlValue.(type) {
				case []byte:
					return v, nil
				case string:
					return []byte(v), nil
				default:
					return []byte(fmt.Sprintf("%v", sqlValue)), nil
				}
			}
		}
		// Fall through to default for non-spatial binary
		return m.DefaultTypeConverter.ConvertSQLToArrow(sqlValue, arrowType)
		
	default:
		// For all other types, use default conversion
		return m.DefaultTypeConverter.ConvertSQLToArrow(sqlValue, arrowType)
	}
}

// Helper function to extract metadata from Arrow type (if available)
func getColumnMetadata(arrowType arrow.DataType) map[string]string {
	// This is a simplified approach - in reality, we'd need access to the field metadata
	// For now, we return nil and rely on type-based detection
	// In a full implementation, we'd pass the arrow.Field to the converter
	return nil
}

// ConvertArrowToGo implements MySQL-specific Arrow value to Go value conversion  
func (m *mySQLTypeConverter) ConvertArrowToGo(arrowArray arrow.Array, index int) (any, error) {
	if arrowArray.IsNull(index) {
		return nil, nil
	}

	// Handle MySQL-specific Arrow to Go conversions
	switch a := arrowArray.(type) {
	case *array.String:
		// Check if this is a JSON column by looking at field metadata
		if field := getFieldFromArray(arrowArray); field != nil {
			if isJSON, ok := field.Metadata.GetValue("mysql.is_json"); ok && isJSON == "true" {
				// For JSON fields, we could parse the JSON here if needed
				jsonStr := a.Value(index)
				// For now, just return the JSON string as-is
				// In a full implementation, we might parse to map[string]interface{}
				return jsonStr, nil
			}
		}
		// Fall through to default string handling
		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index)
		
	case *array.Binary:
		// Check if this is a spatial column
		if field := getFieldFromArray(arrowArray); field != nil {
			if isSpatial, ok := field.Metadata.GetValue("mysql.is_spatial"); ok && isSpatial == "true" {
				// For spatial fields, return the binary data as-is
				// In a full implementation, we might parse WKB to geometry objects
				spatialData := a.Value(index)
				return spatialData, nil
			}
		}
		// Fall through to default binary handling
		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index)
		
	case *array.Timestamp:
		// For timestamp arrays, check if they need timezone conversion
		timestampType := a.DataType().(*arrow.TimestampType)
		value := a.Value(index)
		
		// Check if this is a MySQL TIMESTAMP vs DATETIME
		if field := getFieldFromArray(arrowArray); field != nil {
			if dbType, ok := field.Metadata.GetValue("sql.database_type_name"); ok {
				if strings.ToUpper(dbType) == "TIMESTAMP" {
					// MySQL TIMESTAMP - ensure proper timezone handling
					time := value.ToTime(timestampType.Unit)
					// If the timestamp type has no timezone, assume UTC for MySQL TIMESTAMP
					if timestampType.TimeZone == "" {
						time = time.UTC()
					}
					return time, nil
				}
			}
		}
		// Fall through to default timestamp handling
		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index)
		
	default:
		// For all other types, use default conversion
		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index)
	}
}

// Helper function to get field metadata from array (simplified)
func getFieldFromArray(arrowArray arrow.Array) *arrow.Field {
	// This is a simplified approach - in reality, we'd need to pass the field
	// or have access to the schema context. For now, we return nil.
	// In a full implementation, we'd modify the interface to pass arrow.Field
	return nil
}

// NewDriver constructs the ADBC Driver for "mysql".
func NewDriver() adbc.Driver {
	// Create sqlwrapper driver with MySQL type converter and driver name
	return sqlwrapper.NewDriver("mysql", &mySQLTypeConverter{
		DefaultTypeConverter: sqlwrapper.DefaultTypeConverter{},
	})
}
