package recovery

import (
	"fmt"
	"simpledb/internal/file"
	"simpledb/internal/log"
)

// Check that RollbackRecord implements LogRecord
var _ LogRecord = (*RollbackRecord)(nil)

// RollbackRecord represents a ROLLBACK log record
type RollbackRecord struct {
	txnum int
}

// NewRollbackRecord creates a new RollbackRecord by reading a value from the log.
func NewRollbackRecord(p *file.Page) *RollbackRecord {
	return &RollbackRecord{
		txnum: int(p.GetInt(4)),
	}
}

// Op returns the log record's type.
func (r *RollbackRecord) Op() LogRecordType {
	return Rollback
}

// TxNumber returns the transaction number.
func (r *RollbackRecord) TxNumber() int {
	return r.txnum
}

// Undo does nothing, because a rollback record contains no undo information.
func (r *RollbackRecord) Undo(tx Transaction) error {
	return nil
}

// String returns a string representation of the RollbackRecord.
func (r *RollbackRecord) String() string {
	return fmt.Sprintf("<ROLLBACK %d>", r.txnum)
}

// WriteRollbackToLog writes a rollback record to the log.
// This log record contains the ROLLBACK operator, followed by the transaction id.
// It returns the LSN of the last log value.
func WriteRollbackToLog(lm *log.LogMgr, txnum int) (int, error) {
	rec := make([]byte, 8)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, int32(Rollback))
	p.SetInt(4, int32(txnum))
	return lm.Append(rec)
}
