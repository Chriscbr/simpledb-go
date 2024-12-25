package parse

import (
	"strings"
)

// CreateViewData represents data for the SQL create view statement.
type CreateViewData struct {
	ViewName  string
	QueryData *QueryData
}

// NewCreateViewData creates a new CreateViewData instance with the specified
// view name and view definition.
func NewCreateViewData(viewname string, querydata *QueryData) *CreateViewData {
	return &CreateViewData{
		ViewName:  viewname,
		QueryData: querydata,
	}
}

// String returns a string representation of the command
func (cvd *CreateViewData) String() string {
	var result strings.Builder
	result.WriteString("CREATE VIEW ")
	result.WriteString(cvd.ViewName)
	result.WriteString(" AS ")
	result.WriteString(cvd.QueryData.String())
	return result.String()
}
