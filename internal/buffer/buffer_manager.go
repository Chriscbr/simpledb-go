package buffer

import (
	"errors"
	"fmt"
	"simpledb/internal/file"
	"simpledb/internal/log"
	"sync"
	"time"
)

// BufferMgr manages the pinning and unpinning of buffers to blocks.
type BufferMgr struct {
	bufpool      []*Buffer
	numAvailable int
	mu           sync.Mutex
}

// NewBufferMgr creates a new BufferMgr instance with the given number of
// buffer slots.
func NewBufferMgr(fm *file.FileMgr, lm *log.LogMgr, numbufs int) (*BufferMgr, error) {
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

// Available returns the number of available (i.e. unpinned) buffers.
func (bm *BufferMgr) Available() int {
	bm.mu.Lock()
	a := bm.numAvailable
	bm.mu.Unlock()
	return a
}

// FlushAll flushes the dirty buffers modified by the specified transaction.
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

// Unpin unpins the specified data buffer. If its pin count goes to zero, then
// notify any waiting threads.
func (bm *BufferMgr) Unpin(b *Buffer) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	b.Unpin()
	if !b.IsPinned() {
		bm.numAvailable++
		// notifyAll() // TODO: currently the Pin() method does a wait loop, consider revising and using channels
	}
}

// Pin pins a buffer to the specified block, potentially waiting until a buffer
// becomes available. If no buffer becomes available within a fixed time period,
// a BufferAbortError error is returned.
func (bm *BufferMgr) Pin(blk file.BlockID) (*Buffer, error) {
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

// tryToPin tries to pin a buffer to the specified block. If there is already a
// buffer assigned to that block, then that buffer is used; otherwise, an
// unpinned buffer from the pool is chosen. Returns nil if there are no
// available buffers.
func (bm *BufferMgr) tryToPin(blk file.BlockID) (*Buffer, error) {
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

// findExistingBuffer returns the buffer assigned to the specified block, or nil
// if no buffer is assigned to that block.
func (bm *BufferMgr) findExistingBuffer(blk file.BlockID) *Buffer {
	for _, b := range bm.bufpool {
		if b.Blk.Equal(blk) {
			return b
		}
	}
	return nil
}

// chooseUnpinnedBuffer returns an unpinned buffer from the buffer pool, or nil
// if no such buffer exists.
func (bm *BufferMgr) chooseUnpinnedBuffer() *Buffer {
	for _, b := range bm.bufpool {
		if !b.IsPinned() {
			return b
		}
	}
	return nil
}

// maxWaitTime is the maximum amount of time to wait for a buffer to become
// available.
const maxWaitTime = 10 * time.Second

// waitingTooLong returns true if the specified start time is greater than
// maxWaitTime.
func waitingTooLong(start time.Time) bool {
	return time.Since(start) > maxWaitTime
}
