package record

import (
	"fmt"
	"os"
	"simpledb/internal/file"
	"simpledb/internal/tx"
)

// Scan is the interface for scanning through a table.
type Scan interface {
	// BeforeFirst positions the scan before the first record.
	// A subsequent call to Next() will return the first record.
	BeforeFirst()
	// Next moves the scan to the next record.
	// Returns false if there is no next record.
	Next() bool
	// GetInt returns the value of the specified integer field in the current record.
	GetInt(fldname string) (int32, error)
	// GetString returns the value of the specified string field in the current record.
	GetString(fldname string) (string, error)
	// GetVal returns the value of the specified field in the current record as a Constant.
	GetVal(fldname string) (Constant, error)
	// HasField returns true if the scan has a field with the specified name.
	HasField(fldname string) bool
	// Close closes the scan and its subscans, if any.
	Close()
}

// UpdateScan is the interface for all updateable scans.
type UpdateScan interface {
	Scan
	// SetVal modifies the field value of the current record.
	SetVal(fldname string, val Constant) error
	// SetInt modifies the field value of the current record.
	SetInt(fldname string, val int32) error
	// SetString modifies the field value of the current record.
	SetString(fldname string, val string) error
	// Insert inserts a new record somewhere in the scan.
	Insert() error
	// Delete deletes the current record.
	Delete() error
	// GetRid returns the RID of the current record.
	GetRid() RID
	// MoveToRid positions the scan so that the current record has the specified RID.
	MoveToRid(rid RID)
}

// Check that TableScan implements Scan, UpdateScan
var _ Scan = (*TableScan)(nil)
var _ UpdateScan = (*TableScan)(nil)

// TableScan is used to scan through a table.
// It provides the abstraction of an arbitrarily large array of records.
type TableScan struct {
	tx          *tx.Transaction
	layout      *Layout
	rp          *RecordPage
	filename    string
	currentslot int
}

// NewTableScan creates a new TableScan object.
func NewTableScan(tx *tx.Transaction, tblname string, layout *Layout) (*TableScan, error) {
	filename := fmt.Sprintf("%s.tbl", tblname)
	ts := &TableScan{tx, layout, nil, filename, 0}
	size, err := tx.Size(filename)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		ts.moveToNewBlock()
	} else {
		ts.moveToBlock(0)
	}
	return ts, nil
}

// BeforeFirst moves the table scan before the first record.
func (ts *TableScan) BeforeFirst() {
	ts.moveToBlock(0)
}

// Next moves the table scan to the next record, and returns true if there are
// more records left to scan.
func (ts *TableScan) Next() bool {
	ts.currentslot = ts.rp.NextAfter(ts.currentslot)
	// If there are no more slots in the current record page, look for the next
	// record page with used slots.
	for ts.currentslot < 0 {
		if ok := ts.atLastBlock(); ok {
			return false
		}
		ts.moveToBlock(ts.rp.Blk.Blknum + 1)
		ts.currentslot = ts.rp.NextAfter(ts.currentslot)
	}
	return true
}

// GetInt returns the integer value of the specified field from the current record.
func (ts *TableScan) GetInt(fldname string) (int32, error) {
	return ts.rp.GetInt(ts.currentslot, fldname)
}

// GetString returns the string value of the specified field from the current record.
func (ts *TableScan) GetString(fldname string) (string, error) {
	return ts.rp.GetString(ts.currentslot, fldname)
}

// GetVal returns the value of the specified field from the current record.
func (ts *TableScan) GetVal(fldname string) (Constant, error) {
	typ, err := ts.layout.Schema.Type(fldname)
	if err != nil {
		return Constant{}, err
	}
	switch typ {
	case Integer:
		val, err := ts.rp.GetInt(ts.currentslot, fldname)
		if err != nil {
			return Constant{}, err
		}
		return NewIntConstant(val), nil
	case String:
		val, err := ts.rp.GetString(ts.currentslot, fldname)
		if err != nil {
			return Constant{}, err
		}
		return NewStringConstant(val), nil
	}
	return Constant{}, fmt.Errorf("unknown field type: %v", typ)
}

// HasField returns true if the table has a field with the specified name.
func (ts *TableScan) HasField(fldname string) bool {
	return ts.layout.Schema.HasField(fldname)
}

// Close closes the current table scan.
func (ts *TableScan) Close() {
	if ts.rp != nil {
		ts.tx.Unpin(ts.rp.Blk)
		ts.rp = nil
	}
}

// SetInt sets the value of the specified field in the current record.
func (ts *TableScan) SetInt(fldname string, val int32) error {
	return ts.rp.SetInt(ts.currentslot, fldname, val)
}

// SetString sets the value of the specified field in the current record.
func (ts *TableScan) SetString(fldname string, val string) error {
	return ts.rp.SetString(ts.currentslot, fldname, val)
}

// SetVal sets the value of the specified field in the current record.
func (ts *TableScan) SetVal(fldname string, val Constant) error {
	typ, err := ts.layout.Schema.Type(fldname)
	if err != nil {
		return err
	}
	switch typ {
	case Integer:
		return ts.SetInt(fldname, val.AsInt())
	case String:
		return ts.SetString(fldname, val.AsString())
	}
	return fmt.Errorf("unknown field type: %v", typ)
}

// Insert inserts a new record after the current record in the scan.
func (ts *TableScan) Insert() error {
	ts.currentslot = ts.rp.InsertAfter(ts.currentslot)
	for ts.currentslot < 0 {
		if ok := ts.atLastBlock(); ok {
			err := ts.moveToNewBlock()
			if err != nil {
				return err
			}
		} else {
			ts.moveToBlock(ts.rp.Blk.Blknum + 1)
		}
		ts.currentslot = ts.rp.InsertAfter(ts.currentslot)
	}
	return nil
}

// Delete deletes the current record.
func (ts *TableScan) Delete() error {
	return ts.rp.Delete(ts.currentslot)
}

// GetRid returns the RID of the current record.
func (ts *TableScan) GetRid() RID {
	return NewRID(ts.rp.Blk.Blknum, ts.currentslot)
}

// MoveToRid positions the scan so that the current record has the specified RID.
func (ts *TableScan) MoveToRid(rid RID) {
	ts.Close()
	blk := file.NewBlockID(ts.filename, rid.Blknum)
	ts.rp = NewRecordPage(ts.tx, blk, ts.layout)
	ts.currentslot = rid.Slot
}

// moveToBlock moves the table scan internally to the specified block.
func (ts *TableScan) moveToBlock(blknum int) {
	ts.Close()
	blk := file.NewBlockID(ts.filename, blknum)
	ts.rp = NewRecordPage(ts.tx, blk, ts.layout)
	ts.currentslot = -1
}

// moveToNewBlock moves the table scan to a new, empty block.
func (ts *TableScan) moveToNewBlock() error {
	ts.Close()
	blk, err := ts.tx.Append(ts.filename)
	if err != nil {
		return err
	}
	ts.rp = NewRecordPage(ts.tx, blk, ts.layout)
	err = ts.rp.Format()
	if err != nil {
		return err
	}
	ts.currentslot = -1
	return nil
}

// atLastBlock returns true if the table scan is at the last block
// or if there is an error reading the size of the file.
func (ts *TableScan) atLastBlock() bool {
	numblks, err := ts.tx.Size(ts.filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting size of file %s: %v\n", ts.filename, err)
		return true
	}
	return ts.rp.Blk.Blknum == numblks-1
}
