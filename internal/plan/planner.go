package plan

import (
	"simpledb/internal/parse"
	"simpledb/internal/query"
	"simpledb/internal/tx"
)

// Planner executes SQL statements.
type Planner struct {
	qp QueryPlanner
}

// NewPlanner creates a new Planner.
func NewPlanner(qp QueryPlanner) *Planner {
	return &Planner{qp: qp}
}

// CreateQueryPlan creates a query plan for the given SQL query.
func (p *Planner) CreateQueryPlan(query string, tx *tx.Transaction) (query.Plan, error) {
	lexer := parse.NewLexer(query)
	parser := parse.NewParser(lexer)
	data, err := parser.Query()
	if err != nil {
		return nil, err
	}
	// TODO: verify the query
	return p.qp.CreatePlan(data, tx)
}
