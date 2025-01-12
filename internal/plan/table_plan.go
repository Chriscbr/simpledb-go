package plan

import (
	"simpledb/internal/metadata"
	"simpledb/internal/query"
	"simpledb/internal/record"
	"simpledb/internal/tx"
)

// TablePlan is a plan that corresponds to a table.
type TablePlan struct {
	tblname string
	tx      *tx.Transaction
	layout  *record.Layout
	si      *metadata.StatInfo
}

var _ query.Plan = (*TablePlan)(nil)

// NewTablePlan creates a new TablePlan.
func NewTablePlan(tx *tx.Transaction, tblname string, mdm *metadata.MetadataMgr) (*TablePlan, error) {
	tp := &TablePlan{tblname: tblname, tx: tx}
	layout, err := mdm.GetLayout(tblname, tx)
	if err != nil {
		return nil, err
	}
	tp.layout = layout
	si, err := mdm.GetStatInfo(tblname, tp.layout, tx)
	if err != nil {
		return nil, err
	}
	tp.si = si
	return tp, nil
}

// Open opens a scan for this query.
func (tp *TablePlan) Open() (record.Scan, error) {
	return record.NewTableScan(tp.tx, tp.tblname, tp.layout)
}

// BlocksAccessed returns an estimate of the number of blocks that will be
// accessed by this plan.
func (tp *TablePlan) BlocksAccessed() int {
	return tp.si.BlocksAccessed
}

// RecordsOutput returns an estimate of the number of records that will be
// output by this plan.
func (tp *TablePlan) RecordsOutput() int {
	return tp.si.RecordsOutput
}

// DistinctValues returns an estimate of the number of distinct values for the
// specified field.
func (tp *TablePlan) DistinctValues(fldname string) int {
	return tp.si.DistinctValues(fldname)
}

// Schema returns the schema of this plan.
func (tp *TablePlan) Schema() *record.Schema {
	return tp.layout.Schema
}
