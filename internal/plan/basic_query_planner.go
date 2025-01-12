package plan

import (
	"simpledb/internal/metadata"
	"simpledb/internal/parse"
	"simpledb/internal/query"
	"simpledb/internal/tx"
)

// BasicQueryPlanner is a simple, naive query planner.
type BasicQueryPlanner struct {
	mdm *metadata.MetadataMgr
}

var _ QueryPlanner = (*BasicQueryPlanner)(nil)

// NewBasicQueryPlanner creates a new BasicQueryPlanner.
func NewBasicQueryPlanner(mdm *metadata.MetadataMgr) *BasicQueryPlanner {
	return &BasicQueryPlanner{mdm: mdm}
}

// CreatePlan creates a query plan by first taking the product of all tables
// and views; it then selects on the predicate; and finally it projects
// on the fields list.
func (p *BasicQueryPlanner) CreatePlan(data *parse.QueryData, tx *tx.Transaction) (query.Plan, error) {
	// Step 1: create a plan for each mentioned table or view.
	plans := make([]query.Plan, 0, len(data.Tables))
	for _, tblname := range data.Tables {
		viewdef, err := p.mdm.GetViewDef(tblname, tx)
		if err != nil {
			return nil, err
		}
		if viewdef != "" {
			// Recursively plan the view.
			lexer := parse.NewLexer(viewdef)
			parser := parse.NewParser(lexer)
			viewdata, err := parser.Query()
			if err != nil {
				return nil, err
			}
			plan, err := p.CreatePlan(viewdata, tx)
			if err != nil {
				return nil, err
			}
			plans = append(plans, plan)
		} else {
			plan, err := NewTablePlan(tx, tblname, p.mdm)
			if err != nil {
				return nil, err
			}
			plans = append(plans, plan)
		}
	}

	// Step 2: create the product of all table plans
	plan := plans[0]
	plans = plans[1:]
	for _, nextplan := range plans {
		plan = NewProductPlan(plan, nextplan)
	}

	// Step 3: add a select plan for the predicate
	plan = NewSelectPlan(plan, data.Pred)

	// Step 4: project on the field names
	plan = NewProjectPlan(plan, data.Fields)

	return plan, nil
}
