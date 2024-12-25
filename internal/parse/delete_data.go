package parse

import (
	"strings"

	"simpledb/internal/query"
)

// DeleteData represents data for the SQL delete statement.
type DeleteData struct {
	TableName string
	Pred      *query.Predicate
}

// NewDeleteData creates a new DeleteData instance with the specified table name and predicate.
func NewDeleteData(tableName string, pred *query.Predicate) *DeleteData {
	return &DeleteData{
		TableName: tableName,
		Pred:      pred,
	}
}

// String returns a string representation of the command
func (dd *DeleteData) String() string {
	var result strings.Builder
	result.WriteString("DELETE FROM ")
	result.WriteString(dd.TableName)
	if predString := dd.Pred.String(); predString != "" {
		result.WriteString(" WHERE ")
		result.WriteString(predString)
	}
	return result.String()
}
