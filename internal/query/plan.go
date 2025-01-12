package query

import "simpledb/internal/record"

// Plan is the interface that all plans must implement.
type Plan interface {
	// Open opens a scan corresponding to this plan.
	// The scan will be positioned before its first record.
	Open() (record.Scan, error)

	// BlocksAccessed returns an estimate of the number of disk blocks that
	// are accessed by this plan.
	BlocksAccessed() int

	// RecordsOutput returns an estimate of the number of records in the query's
	// output table.
	RecordsOutput() int

	// DistinctValues returns an estimate of the number of distinct values
	// for the specified field in the query's output table.
	DistinctValues(fldname string) int

	// Schema returns the schema of the query's output table.
	Schema() *record.Schema
}
