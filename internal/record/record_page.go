package record

import (
	"simpledb/internal/file"
	"simpledb/internal/tx"
)

type SlotFlag int

const (
	SlotEmpty SlotFlag = iota
	SlotUsed
)

// RecordPage stores records within a block.
// The records are stored in a contiguous areas of the block, each of the same size,
// called slots.
type RecordPage struct {
	tx     *tx.Transaction
	Blk    file.BlockID
	layout *Layout
}

func NewRecordPage(tx *tx.Transaction, blk file.BlockID, layout *Layout) *RecordPage {
	rp := &RecordPage{tx, blk, layout}
	tx.Pin(blk)
	return rp
}

// GetInt returns the integer value stored for the specified field of a
// specified slot.
func (rp *RecordPage) GetInt(slot int, fldname string) (int32, error) {
	fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
	return rp.tx.GetInt(rp.Blk, fldpos)
}

// GetString returns the string value stored for the specified field of a
// specified slot.
func (rp *RecordPage) GetString(slot int, fldname string) (string, error) {
	fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
	return rp.tx.GetString(rp.Blk, fldpos)
}

// SetInt stores an integer at the specified field of a specified slot.
func (rp *RecordPage) SetInt(slot int, fldname string, val int32) error {
	fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
	return rp.tx.SetInt(rp.Blk, fldpos, val, true)
}

// SetString stores a string at the specified field of a specified slot.
func (rp *RecordPage) SetString(slot int, fldname string, val string) error {
	fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
	return rp.tx.SetString(rp.Blk, fldpos, val, true)
}

// Delete marks a slot as unused.
func (rp *RecordPage) Delete(slot int) error {
	return rp.setFlag(slot, SlotEmpty)
}

// Format uses the table layout to format a new block of records.
func (rp *RecordPage) Format() error {
	slot := 0
	for rp.isValidSlot(slot) {
		// Values are not logged because the old values are meaningless.
		rp.tx.SetInt(rp.Blk, rp.offset(slot), int32(SlotEmpty), false)
		sch := rp.layout.Schema
		for _, fldname := range sch.Fields {
			fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
			typ := sch.Type(fldname)
			var err error
			switch typ {
			case Integer:
				err = rp.tx.SetInt(rp.Blk, fldpos, 0, false)
			case String:
				err = rp.tx.SetString(rp.Blk, fldpos, "", false)
			}
			if err != nil {
				return err
			}
		}
		slot++
	}
	return nil
}

// NextAfter returns the slot number of the next used slot after the specified slot.
// Returns -1 if no such slot exists.
func (rp *RecordPage) NextAfter(slot int) int {
	return rp.searchAfter(slot, SlotUsed)
}

// InsertAfter finds the first unused slot after the specified slot,
// and marks it as used.
// Returns -1 if no slot is available.
func (rp *RecordPage) InsertAfter(slot int) int {
	slot = rp.searchAfter(slot, SlotEmpty)
	if slot >= 0 {
		err := rp.setFlag(slot, SlotUsed)
		if err != nil {
			return -1
		}
	}
	return slot
}

// setFlag sets a record slot's empty/inuse flag.
func (rp *RecordPage) setFlag(slot int, flag SlotFlag) error {
	return rp.tx.SetInt(rp.Blk, rp.offset(slot), int32(flag), true)
}

// searchAfter returns the slot number of the slot after the specified slot
// that has the specified flag.
// Returns -1 if no such slot exists.
func (rp *RecordPage) searchAfter(slot int, flag SlotFlag) int {
	slot++
	for rp.isValidSlot(slot) {
		if val, err := rp.tx.GetInt(rp.Blk, rp.offset(slot)); err == nil && val == int32(flag) {
			return slot
		}
		slot++
	}
	return -1
}

// isValidSlot returns true if the slot is within the range of valid slots.
func (rp *RecordPage) isValidSlot(slot int) bool {
	return rp.offset(slot+1) <= rp.tx.BlockSize()
}

// offset returns the byte offset of a slot within the block.
func (rp *RecordPage) offset(slot int) int {
	return slot * rp.layout.SlotSize
}
