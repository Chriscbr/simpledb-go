package simpledb

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// BufferAbortError represents an error indicating that a buffer request could
// not be satisfied and the transaction needs to abort.
type BufferAbortError struct{}

// Error implements the error interface for BufferAbortError.
func (e *BufferAbortError) Error() string {
	return "no available buffers"
}

// NewBufferAbortError creates a new BufferAbortError with an optional message.
func NewBufferAbortError() *BufferAbortError {
	return &BufferAbortError{}
}

// BufferMgr manages the pinning and unpinning of buffers to blocks.
type BufferMgr struct {
	bufpool      []*Buffer
	numAvailable int
	mu           sync.Mutex
}

// Creates a new BufferMgr instance with the given number of buffer slots.
func NewBufferMgr(fm *FileMgr, lm *LogMgr, numbufs int) (*BufferMgr, error) {
	bufpool := make([]*Buffer, numbufs)
	for i := 0; i < numbufs; i++ {
		bufpool[i] = NewBuffer(fm, lm)
	}
	bm := &BufferMgr{
		bufpool:      bufpool,
		numAvailable: numbufs,
	}
	return bm, nil
}

// Returns the number of available (i.e. unpinned) buffers.
func (bm *BufferMgr) Available() int {
	bm.mu.Lock()
	a := bm.numAvailable
	bm.mu.Unlock()
	return a
}

// Flushes the dirty buffers modified by the specified transaction.
func (bm *BufferMgr) FlushAll(txnum int) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	var errs []error
	for i, b := range bm.bufpool {
		if b.Txnum == txnum {
			if err := b.Flush(); err != nil {
				errs = append(errs, fmt.Errorf("error flushing buffer %d: %w", i, err))
			}
		}
	}
	return errors.Join(errs...)
}

// Unpins the specified data buffer. If its pin count goes to zero, then notify
// any waiting threads.
func (bm *BufferMgr) Unpin(b *Buffer) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	b.Unpin()
	if !b.IsPinned() {
		bm.numAvailable++
		// notifyAll() // TODO: ???
	}
}

// Pins a buffer to the specified block, potentially waiting until a buffer
// becomes available. If no buffer becomes available within a fixed time period,
// a BufferAbortError error is returned.
func (bm *BufferMgr) Pin(blk *BlockId) (*Buffer, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	start := time.Now()
	for {
		b, err := bm.tryToPin(blk)
		if err != nil {
			return nil, err
		}
		if b != nil {
			return b, nil
		}
		if waitingTooLong(start) {
			return nil, NewBufferAbortError()
		}
		// wait a short duration before trying again
		bm.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		bm.mu.Lock()
	}
}

// Tries to pin a buffer to the specified block. If there is already a buffer
// assigned to that block, then that buffer is used; otherwise, an unpinned
// buffer from the pool is chosen. Returns nil if there are
// no available buffers.
func (bm *BufferMgr) tryToPin(blk *BlockId) (*Buffer, error) {
	b := bm.findExistingBuffer(blk)
	if b == nil {
		b = bm.chooseUnpinnedBuffer()
		if b == nil {
			// there is no existing buffer for this block, nor unpinned buffer available
			return nil, nil
		}
		if err := b.AssignToBlock(blk); err != nil {
			return nil, err
		}
	}

	// by this point, b is not nil
	if !b.IsPinned() {
		// if it's not pinned, we are the first to pin it, so there's one less buffer available now
		bm.numAvailable--
	}
	b.Pin()
	return b, nil
}

func (bm *BufferMgr) findExistingBuffer(blk *BlockId) *Buffer {
	for _, b := range bm.bufpool {
		if b.Blk != nil && b.Blk.Equal(blk) {
			return b
		}
	}
	return nil
}

func (bm *BufferMgr) chooseUnpinnedBuffer() *Buffer {
	for _, b := range bm.bufpool {
		if !b.IsPinned() {
			return b
		}
	}
	return nil
}

const maxWaitTime = 10 * time.Second

func waitingTooLong(start time.Time) bool {
	return time.Since(start) > maxWaitTime
}

// An individual buffer. It wraps a page and stores information about its
// status, such as the associated disk block, the number of times the buffer
// has been pinned, whether its contents have been modified, and if so, the id
// and lsn of the modifying txn.
type Buffer struct {
	fm       *FileMgr
	lm       *LogMgr
	Contents *Page
	Blk      *BlockId
	pins     int
	Txnum    int
	lsn      int
}

// Creates a new Buffer instance.
func NewBuffer(fm *FileMgr, lm *LogMgr) *Buffer {
	b := &Buffer{
		fm:       fm,
		lm:       lm,
		Contents: NewPage(fm.BlockSize),
		Blk:      nil,
		pins:     0,
		Txnum:    -1,
		lsn:      -1,
	}
	return b
}

// Marks the buffer as modified by the specified transaction and updates the
// LSN (log sequence number).
func (b *Buffer) SetModified(txnum int, lsn int) {
	b.Txnum = txnum
	if lsn >= 0 {
		b.lsn = lsn
	}
}

// Returns true if the buffer is currently pinned
func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

// Reads the contents of the specified block into the contents of the buffer.
// If the buffer was dirty, then its previous contents are first written to disk.
func (b *Buffer) AssignToBlock(blk *BlockId) error {
	if err := b.Flush(); err != nil {
		return err
	}
	b.Blk = blk
	if err := b.fm.Read(b.Blk, b.Contents); err != nil {
		return err
	}
	b.pins = 0
	return nil
}

// Write the buffer to its disk block if it's dirty.
func (b *Buffer) Flush() error {
	if b.Txnum >= 0 {
		if err := b.lm.Flush(b.lsn); err != nil {
			return err
		}
		if err := b.fm.Write(b.Blk, b.Contents); err != nil {
			return err
		}
		b.Txnum = -1
	}
	return nil
}

// Increase the buffer's pin count
func (b *Buffer) Pin() {
	b.pins += 1
}

// Decrease the buffer's pin count
func (b *Buffer) Unpin() {
	b.pins -= 1
}
