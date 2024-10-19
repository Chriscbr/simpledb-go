package recovery

import (
	"simpledb/internal/file"
	"simpledb/internal/log"
	"simpledb/internal/tx"
)

// CheckpointRecord represents a CHECKPOINT log record.
type CheckpointRecord struct{}

// NewCheckpointRecord creates a new CheckpointRecord.
func NewCheckpointRecord() *CheckpointRecord {
	return &CheckpointRecord{}
}

// Op returns the log record's type.
func (r *CheckpointRecord) Op() LogRecordType {
	return Checkpoint
}

// TxNumber returns a dummy transaction number.
// Checkpoint records have no associated transaction,
// so the method returns a "dummy", negative txid.
func (r *CheckpointRecord) TxNumber() int {
	return -1 // dummy value
}

// Undo does nothing, because a checkpoint record contains no undo information.
func (r *CheckpointRecord) Undo(tx *tx.Transaction) {}

// String returns a string representation of the CheckpointRecord.
func (r *CheckpointRecord) String() string {
	return "<CHECKPOINT>"
}

// WriteCheckpointToLog writes a checkpoint record to the log.
// This log record contains the CHECKPOINT operator, and nothing else.
// It returns the LSN of the last log value.
func WriteCheckpointToLog(lm *log.LogMgr) (int, error) {
	rec := make([]byte, 4)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, int32(Checkpoint))
	return lm.Append(rec)
}
