package plan

import (
	"simpledb/internal/query"
	"simpledb/internal/record"
)

// ProductPlan represents a query plan that computes the product of two
// subqueries.
type ProductPlan struct {
	p1, p2 query.Plan
	schema *record.Schema
}

var _ query.Plan = (*ProductPlan)(nil)

// NewProductPlan creates a new ProductPlan with the specified subqueries.
func NewProductPlan(p1, p2 query.Plan) *ProductPlan {
	schema := &record.Schema{}
	schema.AddAll(p1.Schema())
	schema.AddAll(p2.Schema())
	return &ProductPlan{p1: p1, p2: p2, schema: schema}
}

// Open opens a scan for this query.
func (pp *ProductPlan) Open() (record.Scan, error) {
	s1, err := pp.p1.Open()
	if err != nil {
		return nil, err
	}
	s2, err := pp.p2.Open()
	if err != nil {
		return nil, err
	}
	return query.NewProductScan(s1, s2)
}

// BlocksAccessed estimates the number of blocks that will be
// accessed by this plan.
func (pp *ProductPlan) BlocksAccessed() int {
	return pp.p1.BlocksAccessed() + (pp.p1.RecordsOutput() * pp.p2.BlocksAccessed())
}

// RecordsOutput estimates the number of records that will be output
// by this plan.
func (pp *ProductPlan) RecordsOutput() int {
	return pp.p1.RecordsOutput() * pp.p2.RecordsOutput()
}

// DistinctValues estimates the number of distinct values for the specified
// field in the output of this plan.
func (pp *ProductPlan) DistinctValues(fldname string) int {
	if pp.p1.Schema().HasField(fldname) {
		return pp.p1.DistinctValues(fldname)
	}
	return pp.p2.DistinctValues(fldname)
}

// Schema returns the schema of the output of this plan.
func (pp *ProductPlan) Schema() *record.Schema {
	return pp.schema
}
