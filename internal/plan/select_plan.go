package plan

import (
	"simpledb/internal/query"
	"simpledb/internal/record"
)

// SelectPlan represents a query plan that selects records from a subquery
// based on a predicate.
type SelectPlan struct {
	p    query.Plan
	pred *query.Predicate
}

// NewSelectPlan creates a new SelectPlan with the specified
// subquery and predicate.
func NewSelectPlan(p query.Plan, pred *query.Predicate) *SelectPlan {
	return &SelectPlan{p: p, pred: pred}
}

// Open opens a scan for this query.
func (sp *SelectPlan) Open() (record.Scan, error) {
	scan, err := sp.p.Open()
	if err != nil {
		return nil, err
	}
	return query.NewSelectScan(scan, sp.pred), nil
}

// BlocksAccessed returns an estimate of the number of blocks that will be
// accessed by this plan.
func (sp *SelectPlan) BlocksAccessed() int {
	return sp.p.BlocksAccessed()
}

// RecordsOutput returns an estimate of the number of records that will be
// output by this plan.
func (sp *SelectPlan) RecordsOutput() int {
	factor, err := sp.pred.ReductionFactor(sp.p)
	if err != nil {
		return 0
	}
	return sp.p.RecordsOutput() / factor
}

// DistinctValues returns an estimate of the number of distinct values
// in the projection.
// If the predicate contains a term equating the specified field to a
// constant, then this value will be 1.
// Otherwise, it will be the number of distinct values in the underlying
// query (but not more than the size of the output table).
func (sp *SelectPlan) DistinctValues(fldname string) int {
	if sp.pred.EquatesWithConstant(fldname) != nil {
		return 1
	}
	fldname2 := sp.pred.EquatesWithField(fldname)
	if fldname2 != nil {
		return min(sp.p.DistinctValues(fldname), sp.p.DistinctValues(*fldname2))
	}
	return sp.p.DistinctValues(fldname)
}

// Schema returns the schema of the output of this query.
func (sp *SelectPlan) Schema() *record.Schema {
	return sp.p.Schema()
}
