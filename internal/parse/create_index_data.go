package parse

import (
	"strings"
)

// CreateIndexData represents data for the SQL create index statement.
type CreateIndexData struct {
	IndexName, TableName, FieldName string
}

// NewCreateIndexData creates a new CreateIndexData instance with the specified
// index name, table name, and field name.
func NewCreateIndexData(indexname, tblname, fieldname string) *CreateIndexData {
	return &CreateIndexData{
		IndexName: indexname,
		TableName: tblname,
		FieldName: fieldname,
	}
}

// String returns a string representation of the command
func (cid *CreateIndexData) String() string {
	var result strings.Builder
	result.WriteString("CREATE INDEX ")
	result.WriteString(cid.IndexName)
	result.WriteString(" ON ")
	result.WriteString(cid.TableName)
	result.WriteString(" (")
	result.WriteString(cid.FieldName)
	result.WriteString(")")
	return result.String()
}
