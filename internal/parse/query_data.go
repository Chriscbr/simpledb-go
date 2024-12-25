package parse

import (
	"strings"

	"simpledb/internal/query"
)

// QueryData represents data for the SQL select statement.
type QueryData struct {
	Fields []string
	Tables []string
	Pred   *query.Predicate
}

// NewQueryData creates a new QueryData instance with the specified fields, tables, and predicate.
func NewQueryData(fields []string, tables []string, pred *query.Predicate) *QueryData {
	return &QueryData{
		Fields: fields,
		Tables: tables,
		Pred:   pred,
	}
}

// String returns a string representation of the query
func (q *QueryData) String() string {
	var result strings.Builder
	result.WriteString("SELECT ")
	result.WriteString(strings.Join(q.Fields, ", "))
	result.WriteString(" FROM ")
	result.WriteString(strings.Join(q.Tables, ", "))
	if predString := q.Pred.String(); predString != "" {
		result.WriteString(" WHERE ")
		result.WriteString(predString)
	}
	return result.String()
}
