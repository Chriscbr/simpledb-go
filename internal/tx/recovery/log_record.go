package recovery

import (
	"fmt"
	"simpledb/internal/file"
)

// LogRecordType represents the type of a log record.
type LogRecordType = int

const (
	Checkpoint LogRecordType = 0
	Start      LogRecordType = 1
	Commit     LogRecordType = 2
	Rollback   LogRecordType = 3
	SetInt     LogRecordType = 4
	SetString  LogRecordType = 5
)

// Transaction is an interface used to decouple the recovery package from the tx package.
type Transaction interface {
	Pin(blk file.BlockID) error
	Unpin(blk file.BlockID)
	SetInt(blk file.BlockID, offset int, val int32, okToLog bool)
	SetString(blk file.BlockID, offset int, val string, okToLog bool)
}

// LogRecord is an interface implemented by each type of log record.
type LogRecord interface {
	// Op returns the log record's type.
	Op() LogRecordType
	// TxNumber returns the transaction ID stored with the log record.
	TxNumber() int
	// Undo undoes the operation encoded by this log record, if applicable.
	Undo(tx Transaction) error
}

// CreateLogRecord interprets the bytes returned by the log iterator and creates the appropriate LogRecord
func CreateLogRecord(bytes []byte) (LogRecord, error) {
	p := file.NewPageFromBytes(bytes)
	switch LogRecordType(p.GetInt(0)) {
	case Checkpoint:
		return NewCheckpointRecord(), nil
	case Start:
		return NewStartRecord(p), nil
	case Commit:
		return NewCommitRecord(p), nil
	case Rollback:
		return NewRollbackRecord(p), nil
	case SetInt:
		return NewSetIntRecord(p), nil
	case SetString:
		return NewSetStringRecord(p), nil
	default:
		return nil, fmt.Errorf("unknown log record type: %d", p.GetInt(0))
	}
}
