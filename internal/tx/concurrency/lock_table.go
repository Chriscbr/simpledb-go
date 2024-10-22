package concurrency

import (
	"simpledb/internal/file"
	"sync"
	"time"
)

const maxWaitTime = 10 * time.Second

// LockTable provides methods to lock and unlock blocks.
// If a transaction requests a lock that causes a conflict with an
// existing lock, then that transaction is placed on a wait list.
// There is only one wait list for all blocks.
// When the last block on a block is unlock, then all transactions
// are removed from the wait list and rescheduled.
// If one of those transactions discovers that the lock it is waiting for
// is still locked, it will place itself back on the wait list.
type LockTable struct {
	mu      sync.Mutex
	locks   map[file.BlockID]int
	waiters map[file.BlockID]chan struct{}
}

// NewLockTable creates a new LockTable.
func NewLockTable() *LockTable {
	return &LockTable{
		locks:   make(map[file.BlockID]int),
		waiters: make(map[file.BlockID]chan struct{}),
	}
}

// SLock grants a shared lock on the specified block.
// If an exclusive lock on it already exists when the method is called,
// then the calling goroutine will be placed on a wait list until
// the lock is released. If the goroutine remains on the wait list for
// more than a certain amount of time (currently 10 seconds),
// then the method will return an error.
func (lt *LockTable) SLock(blk file.BlockID) error {
	lt.mu.Lock()

	start := time.Now()
	for lt.locks[blk] == -1 {
		ch := lt.getOrCreateWaitChannel(blk)
		lt.mu.Unlock()

		if time.Since(start) > maxWaitTime {
			return NewLockAbortError()
		}

		// Wait on the channel with a timeout
		select {
		case <-ch:
			// Continue when the lock is released
		case <-time.After(maxWaitTime):
			return NewLockAbortError()
		}

		lt.mu.Lock()
	}
	val := lt.locks[blk] // will not be negative
	lt.locks[blk] = val + 1
	lt.mu.Unlock()
	return nil
}

// XLock grants an exclusive lock on the specified block.
// If a lock of any kind already exists on it when the method is called,
// then the calling goroutine will be placed on a wait list until
// the lock is released. If the goroutine remains on the wait list for
// more than a certain amount of time (currently 10 seconds), then the method
// will return an error.
func (lt *LockTable) XLock(blk file.BlockID) error {
	lt.mu.Lock()

	start := time.Now()

	// We assume the concurrency manager will obtain an SLock
	// before obtaining an XLock, so we only need to wait if
	// another transaction is also holding this block.
	for lt.locks[blk] > 1 {
		ch := lt.getOrCreateWaitChannel(blk)
		lt.mu.Unlock()

		if time.Since(start) > maxWaitTime {
			return NewLockAbortError()
		}

		// Wait on the channel with a timeout
		select {
		case <-ch:
			// Continue when the lock is released
		case <-time.After(maxWaitTime):
			return NewLockAbortError()
		}

		lt.mu.Lock()
	}
	lt.locks[blk] = -1
	lt.mu.Unlock()
	return nil
}

// Unlock releases a lock on the specified block.
// If this lock is the last lock on the block, then all goroutines waiting
// for that lock are removed from the wait list and rescheduled.
func (lt *LockTable) Unlock(blk file.BlockID) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	val := lt.locks[blk]
	if val > 1 {
		lt.locks[blk] = val - 1
	} else {
		delete(lt.locks, blk)
		if ch, exists := lt.waiters[blk]; exists {
			close(ch)               // Signal waiting goroutines
			delete(lt.waiters, blk) // Remove the channel
		}
	}
}

func (lt *LockTable) getOrCreateWaitChannel(blk file.BlockID) chan struct{} {
	if ch, exists := lt.waiters[blk]; exists {
		return ch
	}
	ch := make(chan struct{})
	lt.waiters[blk] = ch
	return ch
}
