package parse

import (
	"strings"

	"simpledb/internal/record"
)

// InsertData represents data for the SQL insert statement.
type InsertData struct {
	TableName string
	Fields    []string
	Values    []record.Constant
}

// NewInsertData creates a new InsertData instance with the specified table name, fields, and values.
func NewInsertData(tblname string, fields []string, values []record.Constant) *InsertData {
	return &InsertData{
		TableName: tblname,
		Fields:    fields,
		Values:    values,
	}
}

// String returns a string representation of the command
func (id *InsertData) String() string {
	var result strings.Builder
	result.WriteString("INSERT INTO ")
	result.WriteString(id.TableName)
	result.WriteString(" (")
	result.WriteString(strings.Join(id.Fields, ", "))
	result.WriteString(") VALUES (")
	for i, value := range id.Values {
		result.WriteString(value.String())
		if i < len(id.Values)-1 {
			result.WriteString(", ")
		}
	}
	result.WriteString(")")
	return result.String()
}
