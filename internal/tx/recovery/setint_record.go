package recovery

import (
	"fmt"
	"simpledb/internal/file"
	"simpledb/internal/log"
)

// Check that SetIntRecord implements LogRecord
var _ LogRecord = (*SetIntRecord)(nil)

// SetIntRecord represents a SETINT log record
type SetIntRecord struct {
	txnum  int
	offset int
	val    int
	blk    file.BlockID
}

// NewSetIntRecord creates a new SetIntRecord by reading values from the log.
func NewSetIntRecord(p *file.Page) *SetIntRecord {
	tpos := 4
	txnum := int(p.GetInt(tpos))
	fpos := tpos + 4
	filename := p.GetString(fpos)
	bpos := fpos + file.MaxLength(len(filename))
	blknum := int(p.GetInt(bpos))
	blk := file.NewBlockID(filename, blknum)
	opos := bpos + 4
	offset := int(p.GetInt(opos))
	vpos := opos + 4
	val := int(p.GetInt(vpos))
	return &SetIntRecord{txnum, offset, val, blk}
}

// Op returns the log record's type.
func (r *SetIntRecord) Op() LogRecordType {
	return SetInt
}

// TxNumber returns the transaction number.
func (r *SetIntRecord) TxNumber() int {
	return r.txnum
}

// Undo replaces the specified data value with the value saved in the log record.
// The method pins a buffer to the specified block, calls SetInt to restore
// the saved value, and unpins the buffer.
func (r *SetIntRecord) Undo(tx Transaction) error {
	err := tx.Pin(r.blk)
	if err != nil {
		return err
	}

	err = tx.SetInt(r.blk, r.offset, int32(r.val), false) // don't log the undo!
	if err != nil {
		return err
	}

	tx.Unpin(r.blk)
	return nil
}

// String returns a string representation of the SetIntRecord.
func (r *SetIntRecord) String() string {
	return fmt.Sprintf("<SETINT %d %s %d %d>", r.txnum, r.blk.String(), r.offset, r.val)
}

// WriteSetIntToLog writes a setint record to the log.
// This log record contains the SETINT operator, followed by the transaction id,
// the filename, number, and offset of the modified block, and the previous integer
// value at that offset.
// It returns the LSN of the last log value.
func WriteSetIntToLog(lm *log.LogMgr, txnum int, blk file.BlockID, offset int, val int) (int, error) {
	tpos := 4
	fpos := tpos + 4
	bpos := fpos + file.MaxLength(len(blk.Filename))
	opos := bpos + 4
	vpos := opos + 4
	rec := make([]byte, vpos+4)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, int32(SetInt))
	p.SetInt(4, int32(txnum))
	p.SetString(fpos, blk.Filename)
	p.SetInt(bpos, int32(blk.Blknum))
	p.SetInt(opos, int32(offset))
	p.SetInt(vpos, int32(val))
	return lm.Append(rec)
}
