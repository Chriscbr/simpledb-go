package plan

import (
	"simpledb/internal/parse"
	"simpledb/internal/query"
	"simpledb/internal/tx"
)

// QueryPlanner is the interface implemented by planners for the
// SQL select statement.
type QueryPlanner interface {
	// CreatePlan creates a plan for the parsed query.
	CreatePlan(data *parse.QueryData, tx *tx.Transaction) (query.Plan, error)
}
