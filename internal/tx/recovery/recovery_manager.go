package recovery

import (
	"simpledb/internal/buffer"
	"simpledb/internal/log"
	"simpledb/internal/tx"
	"slices"
)

// RecoveryMgr is the recovery manager for the transaction system.
// Each transaction has its own recovery manager.
type RecoveryMgr struct {
	lm    *log.LogMgr
	bm    *buffer.BufferMgr
	tx    *tx.Transaction
	txnum int
}

// NewRecoveryMgr creaters a recovery manager for the specified transaction.
func NewRecoveryMgr(tx *tx.Transaction, txnum int, lm *log.LogMgr, bm *buffer.BufferMgr) (*RecoveryMgr, error) {
	rm := &RecoveryMgr{lm, bm, tx, txnum}
	_, err := WriteStartToLog(lm, txnum)
	if err != nil {
		return nil, err
	}
	return rm, nil
}

// Commit writes a commit record to the log, and flushes it to disk.
func (rm *RecoveryMgr) Commit() error {
	err := rm.bm.FlushAll(rm.txnum)
	if err != nil {
		return err
	}

	lsn, err := WriteCommitToLog(rm.lm, rm.txnum)
	if err != nil {
		return err
	}

	err = rm.lm.Flush(lsn)
	if err != nil {
		return err
	}

	return nil
}

// Rollback writes a rollback record to the log and flushes it to disk.
func (rm *RecoveryMgr) Rollback() error {
	err := rm.doRollback()
	if err != nil {
		return err
	}

	err = rm.bm.FlushAll(rm.txnum)
	if err != nil {
		return err
	}

	lsn, err := WriteRollbackToLog(rm.lm, rm.txnum)
	if err != nil {
		return err
	}

	err = rm.lm.Flush(lsn)
	if err != nil {
		return err
	}

	return nil
}

// Recover recovers uncompleted transactions from the log and then writes a
// quiescent checkpoint record to the log and flushes it.
func (rm *RecoveryMgr) Recover() error {
	err := rm.doRecover()
	if err != nil {
		return err
	}

	err = rm.bm.FlushAll(rm.txnum)
	if err != nil {
		return err
	}

	lsn, err := WriteCheckpointToLog(rm.lm)
	if err != nil {
		return err
	}

	err = rm.lm.Flush(lsn)
	if err != nil {
		return err
	}

	return nil
}

// SetInt writes a setint record to the log and returns its LSN.
func (rm *RecoveryMgr) SetInt(b *buffer.Buffer, offset int, newval int) (int, error) {
	// newval isn't used because the recovery algorithm is undo-only
	oldval := b.Contents.GetInt(offset)
	return WriteSetIntToLog(rm.lm, rm.txnum, b.Blk, offset, int(oldval))
}

// SetString writes a setstring record to the log and returns its LSN.
func (rm *RecoveryMgr) SetString(b *buffer.Buffer, offset int, newval string) (int, error) {
	// newvalue isn't used because the recovery algorithm is undo-only
	oldval := b.Contents.GetString(offset)
	return WriteSetStringToLog(rm.lm, rm.txnum, b.Blk, offset, oldval)
}

// doRollback rolls back the transaction by iterating through the
// log records until it finds the transaction's START record, calling
// undo() for each of the transaction's log records.
func (rm *RecoveryMgr) doRollback() error {
	for bytes, err := range rm.lm.All() {
		if err != nil {
			return err
		}
		rec, err := CreateLogRecord(bytes)
		if err != nil {
			return err
		}
		// If the log record is for the current transaction, undo it
		if rec.TxNumber() == rm.txnum {
			// If the log record is a start record, we're done
			if rec.Op() == Start {
				return nil
			}
			// Otherwise, undo the operation
			err = rec.Undo(rm.tx)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// doRecover does a complete database recovery.
//
// The current recovery algorithm is undo-only - a transaction
// never has to be redo-ed because a commit log entry is only written
// after all of the transaction's buffers are flushed to disk.
// The only thing we need to do in a recovery is undo transactions
// that didn't finish complete.
//
// It iterates through the log records (in reverse order), and whenever
// it finds a log record for an unfinished transaction, it calls undo()
// on that record. The method stops when it encounters a CHECKPOINT
// record or the end of the log.
func (rm *RecoveryMgr) doRecover() error {
	finishedTxs := make([]int, 0)
	for bytes, err := range rm.lm.All() {
		if err != nil {
			return err
		}
		rec, err := CreateLogRecord(bytes)
		if err != nil {
			return err
		}
		// If the log record is a checkpoint, the recovery is finished.
		// All logs beyond this point represent completed transations
		// that are already on disk.
		if rec.Op() == Checkpoint {
			return nil
		}
		// If the log record is a commit or rollback,
		// track this as a completed transaction.
		if rec.Op() == Commit || rec.Op() == Rollback {
			finishedTxs = append(finishedTxs, rec.TxNumber())
		}
		// If the log record doesn't match a completed transaction,
		// then the transaction it's part of didn't finish, so undo it.
		if !slices.Contains(finishedTxs, rec.TxNumber()) {
			err = rec.Undo(rm.tx)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
