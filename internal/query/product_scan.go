package query

import (
	"fmt"
	"simpledb/internal/record"
)

// ProductScan is a scan that corresponds to the "product" relational
// algebra operator.
type ProductScan struct {
	s1, s2 record.Scan
}

// Check that ProductScan implements Scan
var _ record.Scan = (*ProductScan)(nil)

// NewProductScan creates a new ProductScan instance.
func NewProductScan(s1, s2 record.Scan) (*ProductScan, error) {
	ps := &ProductScan{s1, s2}
	if err := ps.BeforeFirst(); err != nil {
		return nil, err
	}
	return ps, nil
}

// Scan methods

// BeforeFirst positions the scan before its first record.
// In particular, the LHS scan is positioned at its first record,
// and the RHS scan is positioned before its first record.
func (ps *ProductScan) BeforeFirst() error {
	if err := ps.s1.BeforeFirst(); err != nil {
		return err
	}
	ps.s1.Next()
	if err := ps.s2.BeforeFirst(); err != nil {
		return err
	}
	return nil
}

// Next moves the scan to the next record.
// The method moves to the next RHS record, if possible.
// Otherwise, it moves to the next LHS record and the first RHS record.
// If there are no more LHS records, the method returns false.
func (ps *ProductScan) Next() bool {
	if ps.s2.Next() {
		return true
	}
	if err := ps.s2.BeforeFirst(); err != nil {
		return false
	}
	return ps.s2.Next() && ps.s1.Next()
}

func (ps *ProductScan) GetInt(fldname string) (int32, error) {
	if ps.s1.HasField(fldname) {
		return ps.s1.GetInt(fldname)
	}
	if ps.s2.HasField(fldname) {
		return ps.s2.GetInt(fldname)
	}
	return 0, fmt.Errorf("field %s not found", fldname)
}

func (ps *ProductScan) GetString(fldname string) (string, error) {
	if ps.s1.HasField(fldname) {
		return ps.s1.GetString(fldname)
	}
	if ps.s2.HasField(fldname) {
		return ps.s2.GetString(fldname)
	}
	return "", fmt.Errorf("field %s not found", fldname)
}

func (ps *ProductScan) GetVal(fldname string) (record.Constant, error) {
	if ps.s1.HasField(fldname) {
		return ps.s1.GetVal(fldname)
	}
	if ps.s2.HasField(fldname) {
		return ps.s2.GetVal(fldname)
	}
	return record.Constant{}, fmt.Errorf("field %s not found", fldname)
}

func (ps *ProductScan) Close() {
	ps.s1.Close()
	ps.s2.Close()
}

func (ps *ProductScan) HasField(fldname string) bool {
	return ps.s1.HasField(fldname) || ps.s2.HasField(fldname)
}
