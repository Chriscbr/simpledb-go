package recovery

import (
	"simpledb/internal/file"
	"simpledb/internal/tx"
)

// LogRecordType represents the type of a log record.
type LogRecordType = int

const (
	Checkpoint LogRecordType = 0
	Start                    = 1
	Commit                   = 2
	Rollback                 = 3
	SetInt                   = 4
	SetString                = 5
)

// LogRecord is an interface implemented by each type of log record.
type LogRecord interface {
	// Op returns the log record's type.
	Op() LogRecordType
	// TxNumber returns the transaction ID stored with the log record.
	TxNumber() int
	// Undo undoes the operation encoded by this log record, if applicable.
	Undo(tx *tx.Transaction)
}

// CreateLogRecord interprets the bytes returned by the log iterator and creates the appropriate LogRecord
func CreateLogRecord(bytes []byte) LogRecord {
	p := file.NewPageFromBytes(bytes)
	switch LogRecordType(p.GetInt(0)) {
	case Checkpoint:
		return NewCheckpointRecord()
	case Start:
		return NewStartRecord(p)
	case Commit:
		return NewCommitRecord(p)
	// case Rollback:
	// 	return NewRollbackRecord(p)
	// case SetInt:
	// 	return NewSetIntRecord(p)
	// case SetString:
	// 	return NewSetStringRecord(p)
	default:
		return nil
	}
}
