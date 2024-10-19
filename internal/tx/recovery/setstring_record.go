package recovery

import (
	"fmt"
	"simpledb/internal/file"
	"simpledb/internal/log"
)

// Check that SetStringRecord implements LogRecord
var _ LogRecord = (*SetStringRecord)(nil)

// SetStringRecord represents a SETSTRING log record
type SetStringRecord struct {
	txnum  int
	offset int
	val    string
	blk    file.BlockID
}

// NewSetStringRecord creates a new SetStringRecord by reading values from the log.
func NewSetStringRecord(p *file.Page) *SetStringRecord {
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
	val := p.GetString(vpos)
	return &SetStringRecord{txnum, offset, val, blk}
}

// Op returns the log record's type.
func (r *SetStringRecord) Op() LogRecordType {
	return SetString
}

// TxNumber returns the transaction number.
func (r *SetStringRecord) TxNumber() int {
	return r.txnum
}

// Undo replaces the specified data value with the value saved in the log record.
// The method pins a buffer to the specified block, calls SetString to restore
// the saved value, and unpins the buffer.
func (r *SetStringRecord) Undo(tx Transaction) error {
	err := tx.Pin(r.blk)
	if err != nil {
		return err
	}

	err = tx.SetString(r.blk, r.offset, r.val, false) // don't log the undo!
	if err != nil {
		return err
	}

	tx.Unpin(r.blk)
	return nil
}

// String returns a string representation of the SetStringRecord.
func (r *SetStringRecord) String() string {
	return fmt.Sprintf("<SETSTRING %d %s %d %s>", r.txnum, r.blk.String(), r.offset, r.val)
}

// WriteSetStringToLog writes a setstring record to the log.
// This log record contains the SetString operator, followed by the transaction id,
// the filename, number, and offset of the modified block, and the previous string
// value at that offset.
// It returns the LSN of the last log value.
func WriteSetStringToLog(lm *log.LogMgr, txnum int, blk file.BlockID, offset int, val string) (int, error) {
	tpos := 4
	fpos := tpos + 4
	bpos := fpos + file.MaxLength(len(blk.Filename))
	opos := bpos + 4
	vpos := opos + 4
	reclen := vpos + file.MaxLength(len(val))
	rec := make([]byte, reclen)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, int32(SetString))
	p.SetInt(4, int32(txnum))
	p.SetString(fpos, blk.Filename)
	p.SetInt(bpos, int32(blk.Blknum))
	p.SetInt(opos, int32(offset))
	p.SetString(vpos, val)
	return lm.Append(rec)
}
