package recovery

import (
	"fmt"
	"simpledb/internal/file"
	"simpledb/internal/log"
)

// Check that StartRecord implements LogRecord
var _ LogRecord = (*StartRecord)(nil)

// StartRecord represents a START log record
type StartRecord struct {
	txnum int
}

// NewStartRecord creates a new StartRecord by reading a value from the log.
func NewStartRecord(p *file.Page) *StartRecord {
	return &StartRecord{
		txnum: int(p.GetInt(4)),
	}
}

// Op returns the log record's type.
func (r *StartRecord) Op() LogRecordType {
	return Start
}

// TxNumber returns the transaction number.
func (r *StartRecord) TxNumber() int {
	return r.txnum
}

// Undo does nothing, because a start record contains no undo information.
func (r *StartRecord) Undo(tx Transaction) error {
	return nil
}

// String returns a string representation of the StartRecord.
func (r *StartRecord) String() string {
	return fmt.Sprintf("<START %d>", r.txnum)
}

// WriteStartToLog writes a start record to the log.
// This log record contains the START operator, followed by the transaction id.
// It returns the LSN of the last log value.
func WriteStartToLog(lm *log.LogMgr, txnum int) (int, error) {
	rec := make([]byte, 8)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, int32(Start))
	p.SetInt(4, int32(txnum))
	return lm.Append(rec)
}
