package plan

import (
	"simpledb/internal/metadata"
	"simpledb/internal/parse"
	"simpledb/internal/query"
	"simpledb/internal/record"
	"simpledb/internal/tx"
)

// BasicUpdatePlanner is a basic planner for SQL update statements.
type BasicUpdatePlanner struct {
	mdm *metadata.MetadataMgr
}

var _ UpdatePlanner = (*BasicUpdatePlanner)(nil)

// NewBasicUpdatePlanner creates a new BasicUpdatePlanner.
func NewBasicUpdatePlanner(mdm *metadata.MetadataMgr) *BasicUpdatePlanner {
	return &BasicUpdatePlanner{mdm: mdm}
}

// ExecuteDelete creates a plan for a delete statement.
func (p *BasicUpdatePlanner) ExecuteDelete(data *parse.DeleteData, tx *tx.Transaction) (int, error) {
	var plan query.Plan
	plan, err := NewTablePlan(tx, data.TableName, p.mdm)
	if err != nil {
		return 0, err
	}
	plan = NewSelectPlan(plan, data.Pred)
	s, err := plan.Open()
	us := s.(record.UpdateScan)
	count := 0
	for us.Next() {
		us.Delete()
		count++
	}
	us.Close()
	return count, nil
}

// ExecuteUpdate creates a plan for an update statement.
func (p *BasicUpdatePlanner) ExecuteUpdate(data *parse.UpdateData, tx *tx.Transaction) (int, error) {
	var plan query.Plan
	plan, err := NewTablePlan(tx, data.TableName, p.mdm)
	if err != nil {
		return 0, err
	}
	plan = NewSelectPlan(plan, data.Pred)
	s, err := plan.Open()
	us := s.(record.UpdateScan)
	count := 0
	for us.Next() {
		val, err := data.NewValue.Evaluate(us)
		if err != nil {
			return 0, err
		}
		us.SetVal(data.TargetField, val)
		count++
	}
	us.Close()
	return count, nil
}

// ExecuteInsert creates a plan for an insert statement.
func (p *BasicUpdatePlanner) ExecuteInsert(data *parse.InsertData, tx *tx.Transaction) (int, error) {
	plan, err := NewTablePlan(tx, data.TableName, p.mdm)
	if err != nil {
		return 0, err
	}
	s, err := plan.Open()
	us := s.(record.UpdateScan)
	us.Insert()
	for i, val := range data.Values {
		us.SetVal(data.Fields[i], val)
	}
	us.Close()
	return 1, nil
}

// ExecuteCreateTable creates a plan for a create table statement.
func (p *BasicUpdatePlanner) ExecuteCreateTable(data *parse.CreateTableData, tx *tx.Transaction) (int, error) {
	if err := p.mdm.CreateTable(data.TableName, data.Schema, tx); err != nil {
		return 0, err
	}
	return 0, nil
}

// ExecuteCreateView creates a plan for a create view statement.
func (p *BasicUpdatePlanner) ExecuteCreateView(data *parse.CreateViewData, tx *tx.Transaction) (int, error) {
	if err := p.mdm.CreateView(data.ViewName, data.QueryData.String(), tx); err != nil {
		return 0, err
	}
	return 0, nil
}

// ExecuteCreateIndex creates a plan for a create index statement.
func (p *BasicUpdatePlanner) ExecuteCreateIndex(data *parse.CreateIndexData, tx *tx.Transaction) (int, error) {
	if err := p.mdm.CreateIndex(data.IndexName, data.TableName, data.FieldName, tx); err != nil {
		return 0, err
	}
	return 0, nil
}
