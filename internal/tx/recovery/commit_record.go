package recovery

import (
	"fmt"
	"simpledb/internal/file"
	"simpledb/internal/log"
	"simpledb/internal/tx"
)

// Check that CommitRecord implements LogRecord
var _ LogRecord = (*CommitRecord)(nil)

// CommitRecord represents a COMMIT log record
type CommitRecord struct {
	txnum int
}

// NewCommitRecord creates a new CommitRecord by reading a value from the log.
func NewCommitRecord(p *file.Page) *CommitRecord {
	return &CommitRecord{
		txnum: int(p.GetInt(4)),
	}
}

// Op returns the log record's type.
func (r *CommitRecord) Op() LogRecordType {
	return Commit
}

// TxNumber returns the transaction number.
func (r *CommitRecord) TxNumber() int {
	return r.txnum
}

// Undo does nothing, because a commit record contains no undo information.
func (r *CommitRecord) Undo(tx *tx.Transaction) error {
	return nil
}

// String returns a string representation of the CommitRecord.
func (r *CommitRecord) String() string {
	return fmt.Sprintf("<COMMIT %d>", r.txnum)
}

// WriteCommitToLog writes a commit record to the log.
// This log record contains the COMMIT operator, followed by the transaction id.
// It returns the LSN of the last log value.
func WriteCommitToLog(lm *log.LogMgr, txnum int) (int, error) {
	rec := make([]byte, 8)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, int32(Commit))
	p.SetInt(4, int32(txnum))
	return lm.Append(rec)
}
