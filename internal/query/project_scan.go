package query

import (
	"fmt"
	"simpledb/internal/record"
	"slices"
)

// ProjectScan is a scan that corresponds to the "project" relational
// algebra operator.
// All methods except HasField() delegate their work to the underlying scan.
type ProjectScan struct {
	s      record.Scan
	fields []string
}

// Check that ProjectScan implements Scan
var _ record.Scan = (*ProjectScan)(nil)

// NewProjectScan creates a new ProjectScan instance.
func NewProjectScan(s record.Scan, fields []string) *ProjectScan {
	return &ProjectScan{s: s, fields: fields}
}

// Scan methods

func (ps *ProjectScan) BeforeFirst() error {
	return ps.s.BeforeFirst()
}

func (ps *ProjectScan) Next() bool {
	return ps.s.Next()
}

func (ps *ProjectScan) GetInt(fldname string) (int32, error) {
	if !ps.HasField(fldname) {
		return 0, fmt.Errorf("field %s not found", fldname)
	}
	return ps.s.GetInt(fldname)
}

func (ps *ProjectScan) GetString(fldname string) (string, error) {
	if !ps.HasField(fldname) {
		return "", fmt.Errorf("field %s not found", fldname)
	}
	return ps.s.GetString(fldname)
}

func (ps *ProjectScan) GetVal(fldname string) (record.Constant, error) {
	if !ps.HasField(fldname) {
		return record.Constant{}, fmt.Errorf("field %s not found", fldname)
	}
	return ps.s.GetVal(fldname)
}

func (ps *ProjectScan) Close() {
	ps.s.Close()
}

func (ps *ProjectScan) HasField(fldname string) bool {
	return slices.Contains(ps.fields, fldname)
}
