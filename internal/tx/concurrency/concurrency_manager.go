package concurrency

import (
	"simpledb/internal/file"
)

type LockType int

const (
	SharedLock    LockType = 1
	ExclusiveLock LockType = 2
)

type ConcurrencyMgr struct {
	lt    *LockTable
	locks map[file.BlockID]LockType
}

// NewConcurrencyMgr creates a new ConcurrencyMgr that references the given
// database-wide lock table.
func NewConcurrencyMgr(lt *LockTable) *ConcurrencyMgr {
	return &ConcurrencyMgr{
		lt:    lt,
		locks: make(map[file.BlockID]LockType),
	}
}

// SLock obtains a shared lock on the specified block.
// The method will ask the lock table for an SLock
// if the transaction currently has no locks on the block.
func (cm *ConcurrencyMgr) SLock(blk file.BlockID) error {
	if _, ok := cm.locks[blk]; !ok {
		if err := cm.lt.SLock(blk); err != nil {
			return err
		}
		cm.locks[blk] = SharedLock
	}
	return nil
}

// XLock obtains an exclusive lock on the specified block.
// If the transaction doesn't already have an XLock on the block,
// it will first get an SLock on the block (if necessary) and then
// upgrade it to an XLock.
// This could result in deadlock if two transactions both try
// upgrading from an SLock to an XLock for the same block.
func (cm *ConcurrencyMgr) XLock(blk file.BlockID) error {
	lock := cm.locks[blk]
	if lock != ExclusiveLock {
		if err := cm.SLock(blk); err != nil {
			return err
		}
		if err := cm.lt.XLock(blk); err != nil {
			return err
		}
		cm.locks[blk] = ExclusiveLock
	}
	return nil
}

// Release releases all locks by asking the lock table to unlock each one.
func (cm *ConcurrencyMgr) Release() {
	for blk := range cm.locks {
		cm.lt.Unlock(blk)
	}
	cm.locks = make(map[file.BlockID]LockType)
}
