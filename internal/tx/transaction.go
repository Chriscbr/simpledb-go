package tx

import (
	"fmt"
	"simpledb/internal/buffer"
	"simpledb/internal/file"
	"simpledb/internal/log"
	"simpledb/internal/tx/recovery"
	"sync/atomic"
)

var (
	nextTxNum atomic.Uint32
	endOfFile int = -1
)

// nextTxNumber generates the next transaction number.
func nextTxNumber() int {
	return int(nextTxNum.Add(1))
}

// Transaction provides transaction management for clients, ensuring that
// all transactions are serializable, recoverable, and in general satisfy
// the ACID properties.
type Transaction struct {
	rm      *recovery.RecoveryMgr
	bm      *buffer.BufferMgr
	fm      *file.FileMgr
	txnum   int
	buffers *BufferList
}

// NewTransaction creates a new transaction instance.
func NewTransaction(fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr) (*Transaction, error) {
	t := &Transaction{
		rm:      nil,
		bm:      bm,
		fm:      fm,
		txnum:   nextTxNumber(),
		buffers: NewBufferList(bm),
	}

	rm, err := recovery.NewRecoveryMgr(t, t.txnum, lm, bm)
	if err != nil {
		return nil, err
	}
	t.rm = rm

	return t, nil
}

// Commit commits the transaction. It flushes all modified buffers
// (and their log records), writes and flushes a commit record to the log,
// releases all locks, and unpins any pinned buffers.
func (t *Transaction) Commit() error {
	err := t.rm.Commit()
	if err != nil {
		return err
	}

	fmt.Printf("transaction %d committed\n", t.txnum)
	// TODO: t.cm.Release()
	t.buffers.UnpinAll()
	return nil
}

// Rollback rolls back the current transaction.
// It undoes any modified values, flushes those buffers,
// writes and flushes a rollback record to the log, releases all locks,
// and unpins any pinned buffers.
func (t *Transaction) Rollback() error {
	err := t.rm.Rollback()
	if err != nil {
		return err
	}

	fmt.Printf("transaction %d rolled back\n", t.txnum)
	// TODO: t.cm.Release()
	t.buffers.UnpinAll()
	return nil
}

// Recover flushes all modified buffers, then goes through the log
// and rolls back all uncommitted transactions.
// Finally, it writes a quiescent checkpoint record to the log.
// This method is called during system startup, before user
// transactions begin.
func (t *Transaction) Recover() error {
	err := t.bm.FlushAll(t.txnum)
	if err != nil {
		return err
	}

	err = t.rm.Recover()
	if err != nil {
		return err
	}

	return nil
}

// Pin pins the specified block.
// The transaction managers the buffer for the client.
func (t *Transaction) Pin(blk file.BlockID) error {
	return t.buffers.Pin(blk)
}

// Unpin unpins the specified block.
// The transaction looks up the buffer pinned to this block, and unpins it.
func (t *Transaction) Unpin(blk file.BlockID) {
	t.buffers.Unpin(blk)
}

// Returns the integer value stored at the specified offset
// of the specified block.
func (t *Transaction) GetInt(blk file.BlockID, offset int) int32 {
	// TODO: t.cm.SLock(blk)
	b := t.buffers.GetBuffer(blk)
	return b.Contents.GetInt(offset)
}

// Returns the string value stored at the specified offset
// of the specified block.
func (t *Transaction) GetString(blk file.BlockID, offset int) string {
	// TODO: t.cm.SLock(blk)
	b := t.buffers.GetBuffer(blk)
	return b.Contents.GetString(offset)
}

func (t *Transaction) SetInt(blk file.BlockID, offset int, value int32, okToLog bool) {
	// TODO: implement
}

func (t *Transaction) SetString(blk file.BlockID, offset int, value string, okToLog bool) {
	// TODO: implement
}

func (t *Transaction) AvailableBufs() int {
	// TODO: implement
	return 0
}

func (t *Transaction) Size(filename string) int {
	// TODO: implement
	return 0
}

func (t *Transaction) Append(filename string) file.BlockID {
	// TODO: implement
	return file.BlockID{}
}

func (t *Transaction) BlockSize() int {
	// TODO: implement
	return 0
}
