package parse

import (
	"strconv"
	"strings"

	"simpledb/internal/record"
)

// CreateTableData represents data for the SQL create table statement.
type CreateTableData struct {
	TableName string
	Schema    *record.Schema
}

// NewCreateTableData creates a new CreateTableData instance with the specified
// table name and schema.
func NewCreateTableData(tblname string, schema *record.Schema) *CreateTableData {
	return &CreateTableData{
		TableName: tblname,
		Schema:    schema,
	}
}

// String returns a string representation of the command
func (ctd *CreateTableData) String() string {
	var result strings.Builder
	result.WriteString("CREATE TABLE ")
	result.WriteString(ctd.TableName)
	result.WriteString(" (")
	for i, field := range ctd.Schema.Fields {
		result.WriteString(field)
		result.WriteString(" ")
		typ := ctd.Schema.Type(field)
		result.WriteString(typ.String())
		if typ == record.String {
			result.WriteString("(")
			result.WriteString(strconv.Itoa(ctd.Schema.Length(field)))
			result.WriteString(")")
		}
		if i < len(ctd.Schema.Fields)-1 {
			result.WriteString(", ")
		}
	}
	result.WriteString(")")
	return result.String()
}
