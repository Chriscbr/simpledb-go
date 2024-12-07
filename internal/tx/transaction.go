package tx

import (
	"fmt"
	"simpledb/internal/buffer"
	"simpledb/internal/file"
	"simpledb/internal/log"
	"simpledb/internal/tx/concurrency"
	"simpledb/internal/tx/recovery"
	"sync/atomic"
)

// TODO: fix nextTxNum is shared across test suites

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
	cm      *concurrency.ConcurrencyMgr
	bm      *buffer.BufferMgr
	fm      *file.FileMgr
	txnum   int
	buffers *BufferList
}

// NewTransaction creates a new transaction instance.
func NewTransaction(fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr, lt *concurrency.LockTable) (*Transaction, error) {
	t := &Transaction{
		rm:      nil,
		cm:      concurrency.NewConcurrencyMgr(lt),
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
	t.cm.Release()
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
	t.cm.Release()
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

// GetInt returns the integer value stored at the specified offset
// of the specified block.
// The method first obtains an SLock on the block, then it calls the
// buffer to retrieve the value.
func (t *Transaction) GetInt(blk file.BlockID, offset int) (int32, error) {
	if err := t.cm.SLock(blk); err != nil {
		return 0, err
	}
	b := t.buffers.GetBuffer(blk)
	return b.Contents.GetInt(offset), nil
}

// GetString returns the string value stored at the specified offset
// of the specified block.
// The method first obtains an SLock on the block, then it calls the
// buffer to retrieve the value.
func (t *Transaction) GetString(blk file.BlockID, offset int) (string, error) {
	if err := t.cm.SLock(blk); err != nil {
		return "", err
	}
	b := t.buffers.GetBuffer(blk)
	return b.Contents.GetString(offset), nil
}

// SetInt stores an integer at the specified offset of the specified block.
// The method first obtains an XLock on the block.
// It then reads the current value at that offset, puts it into an
// update record, and writes that record to the log.
// Finally, it calls the buffer to store the value,
// passing in the LSN of the log record and the transaction's id.
func (t *Transaction) SetInt(blk file.BlockID, offset int, n int32, okToLog bool) error {
	if err := t.cm.XLock(blk); err != nil {
		return err
	}
	b := t.buffers.GetBuffer(blk)
	lsn := -1
	if okToLog {
		var err error
		lsn, err = t.rm.SetInt(b, offset, n)
		if err != nil {
			return err
		}
	}
	p := b.Contents
	p.SetInt(offset, n)
	b.SetModified(t.txnum, lsn)
	return nil
}

// SetString stores a string at the specified offset of the specified block.
// The method first obtains an XLock on the block.
// It then reads the current value at that offset, puts it into an
// update record, and writes that record to the log.
// Finally, it calls the buffer to store the value,
// passing in the LSN of the log record and the transaction's id.
func (t *Transaction) SetString(blk file.BlockID, offset int, val string, okToLog bool) error {
	if err := t.cm.XLock(blk); err != nil {
		return err
	}
	b := t.buffers.GetBuffer(blk)
	lsn := -1
	if okToLog {
		var err error
		lsn, err = t.rm.SetString(b, offset, val)
		if err != nil {
			return err
		}
	}
	p := b.Contents
	p.SetString(offset, val)
	b.SetModified(t.txnum, lsn)
	return nil
}

// Size returns the number of blocks in the specified file.
// It first obtains an SLock on the "end of the file",
// before asking the file manager to return the file size.
// It returns an error if a lock could not be obtained.
func (t *Transaction) Size(filename string) (int, error) {
	dummyblk := file.NewBlockID(filename, endOfFile)
	if err := t.cm.SLock(dummyblk); err != nil {
		return 0, err
	}
	return t.fm.Length(filename)
}

// Append appends a new block to the end of the specified file,
// and returns a reference to it.
// This method first obtains an XLock on the "end of the file",
// before performing the append.
// It returns an error if a lock could not be obtained.
func (t *Transaction) Append(filename string) (file.BlockID, error) {
	dummyblk := file.NewBlockID(filename, endOfFile)
	if err := t.cm.XLock(dummyblk); err != nil {
		return file.BlockID{}, err
	}
	return t.fm.Append(filename)
}

// BlockSize returns the block size for the file system.
func (t *Transaction) BlockSize() int {
	return t.fm.BlockSize
}

func (t *Transaction) AvailableBufs() int {
	return t.bm.Available()
}
