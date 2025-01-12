package plan

import (
	"simpledb/internal/query"
	"simpledb/internal/record"
)

// ProjectPlan represents a query plan that projects a subset of fields
// from the output of another query plan.
type ProjectPlan struct {
	p      query.Plan
	schema *record.Schema
}

var _ query.Plan = (*ProjectPlan)(nil)

// NewProjectPlan creates a new ProjectPlan with the specified subquery and
// field names.
func NewProjectPlan(p query.Plan, fieldnames []string) *ProjectPlan {
	schema := record.NewSchema()
	for _, fldname := range fieldnames {
		schema.Add(fldname, p.Schema())
	}
	return &ProjectPlan{p, schema}
}

// Open opens a scan for this query.
func (pp *ProjectPlan) Open() (record.Scan, error) {
	s, err := pp.p.Open()
	if err != nil {
		return nil, err
	}
	return query.NewProjectScan(s, pp.schema.Fields), nil
}

// BlocksAccessed estimates the number of blocks that will be accessed by this plan.
func (pp *ProjectPlan) BlocksAccessed() int {
	return pp.p.BlocksAccessed()
}

// RecordsOutput estimates the number of records that will be output by this plan.
func (pp *ProjectPlan) RecordsOutput() int {
	return pp.p.RecordsOutput()
}

// DistinctValues estimates the number of distinct values for the specified
// field in the output of this plan.
func (pp *ProjectPlan) DistinctValues(fldname string) int {
	return pp.p.DistinctValues(fldname)
}

// Schema returns the schema of the output of this plan.
func (pp *ProjectPlan) Schema() *record.Schema {
	return pp.schema
}
