package plan

import (
	"errors"
	"simpledb/internal/parse"
	"simpledb/internal/query"
	"simpledb/internal/tx"
)

// Planner executes SQL statements.
type Planner struct {
	qp QueryPlanner
	up UpdatePlanner
}

// NewPlanner creates a new Planner.
func NewPlanner(qp QueryPlanner, up UpdatePlanner) *Planner {
	return &Planner{qp: qp, up: up}
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

// ExecuteUpdate executes a SQL insert, delete, modify, or create statement.
// The method dispatches to the appropriate method of the supplied
// update planner, depending on what the parser returns.
// It returns the number of records affected by the update.
func (p *Planner) ExecuteUpdate(query string, tx *tx.Transaction) (int, error) {
	lexer := parse.NewLexer(query)
	parser := parse.NewParser(lexer)
	cmd, err := parser.UpdateCmd()
	if err != nil {
		return 0, err
	}
	// TODO: verify the query
	if insertCmd, ok := cmd.(*parse.InsertData); ok {
		return p.up.ExecuteInsert(insertCmd, tx)
	}
	if deleteCmd, ok := cmd.(*parse.DeleteData); ok {
		return p.up.ExecuteDelete(deleteCmd, tx)
	}
	if updateCmd, ok := cmd.(*parse.UpdateData); ok {
		return p.up.ExecuteUpdate(updateCmd, tx)
	}
	if createTableCmd, ok := cmd.(*parse.CreateTableData); ok {
		return p.up.ExecuteCreateTable(createTableCmd, tx)
	}
	if createViewCmd, ok := cmd.(*parse.CreateViewData); ok {
		return p.up.ExecuteCreateView(createViewCmd, tx)
	}
	if createIndexCmd, ok := cmd.(*parse.CreateIndexData); ok {
		return p.up.ExecuteCreateIndex(createIndexCmd, tx)
	}
	return 0, errors.New("invalid update command")
}
