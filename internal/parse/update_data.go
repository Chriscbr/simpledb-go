package parse

import (
	"strings"

	"simpledb/internal/query"
)

// UpdateData represents data for the SQL update statement.
type UpdateData struct {
	TableName   string
	TargetField string
	NewValue    query.Expression
	Pred        *query.Predicate
}

// NewUpdateData creates a new UpdateData instance with the specified
// table name, target field, new value, and predicate.
func NewUpdateData(tblname string, fldname string, newval query.Expression, pred *query.Predicate) *UpdateData {
	return &UpdateData{
		TableName:   tblname,
		TargetField: fldname,
		NewValue:    newval,
		Pred:        pred,
	}
}

// String returns a string representation of the command
func (ud *UpdateData) String() string {
	var result strings.Builder
	result.WriteString("UPDATE ")
	result.WriteString(ud.TableName)
	result.WriteString(" SET ")
	result.WriteString(ud.TargetField)
	result.WriteString(" = ")
	result.WriteString(ud.NewValue.String())
	if ud.Pred != nil && ud.Pred.String() != "" {
		result.WriteString(" WHERE ")
		result.WriteString(ud.Pred.String())
	}
	return result.String()
}
