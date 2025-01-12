package plan

import (
	"simpledb/internal/parse"
	"simpledb/internal/tx"
)

// UpdatePlanner is the interface implemented by planners for the
// SQL update statement.
type UpdatePlanner interface {
	// ExecuteInsert creates a plan for an insert statement,
	// returning the number of affected records.
	ExecuteInsert(data *parse.InsertData, tx *tx.Transaction) (int, error)

	// ExecuteDelete creates a plan for a delete statement,
	// returning the number of affected records.
	ExecuteDelete(data *parse.DeleteData, tx *tx.Transaction) (int, error)

	// ExecuteUpdate creates a plan for an update statement,
	// returning the number of affected records.
	ExecuteUpdate(data *parse.UpdateData, tx *tx.Transaction) (int, error)

	// ExecuteCreateTable creates a plan for a create table statement,
	// returning the number of affected records.
	ExecuteCreateTable(data *parse.CreateTableData, tx *tx.Transaction) (int, error)

	// ExecuteCreateView creates a plan for a create view statement,
	// returning the number of affected records.
	ExecuteCreateView(data *parse.CreateViewData, tx *tx.Transaction) (int, error)

	// ExecuteCreateIndex creates a plan for a create index statement,
	// returning the number of affected records.
	ExecuteCreateIndex(data *parse.CreateIndexData, tx *tx.Transaction) (int, error)
}
