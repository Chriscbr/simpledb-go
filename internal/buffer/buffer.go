package buffer

import (
	"errors"
	"simpledb/internal/file"
	"simpledb/internal/log"
)

// Buffer is an individual buffer. It wraps a page and stores information about
// its status, such as the associated disk block, the number of times the buffer
// has been pinned, whether its contents have been modified, and if so, the id
// and lsn of the modifying txn.
type Buffer struct {
	fm       *file.FileMgr
	lm       *log.LogMgr
	Contents *file.Page
	Blk      file.BlockId
	pins     int
	Txnum    int
	// The most recent LSN (log sequence number) associated with the buffer,
	// or -1 if the buffer has not been modified.
	// We don't need to store all LSNs, since flushing the log with a given LSN
	// will also write any logs up to that LSN to disk.
	lsn int
}

// NewBuffer creates a new Buffer instance.
func NewBuffer(fm *file.FileMgr, lm *log.LogMgr) *Buffer {
	return &Buffer{
		fm:       fm,
		lm:       lm,
		Contents: file.NewPage(fm.BlockSize),
		Blk:      file.BlockId{},
		pins:     0,
		Txnum:    -1,
		lsn:      -1,
	}
}

// SetModified marks the buffer as modified by the specified transaction and
// updates the most recent LSN (log sequence number) associated with the buffer.
func (b *Buffer) SetModified(txnum int, lsn int) {
	b.Txnum = txnum
	if lsn >= 0 {
		b.lsn = lsn
	}
}

// IsPinned returns true if the buffer is currently pinned
func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

// AssignToBlock reads the contents of the specified block into the contents of
// the buffer. If the buffer was dirty, then its previous contents are first
// written to disk.
func (b *Buffer) AssignToBlock(blk file.BlockId) error {
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

// Flush writes the buffer to its disk block if it's dirty.
func (b *Buffer) Flush() error {
	if b.Txnum >= 0 {
		if err := b.lm.Flush(b.lsn); err != nil {
			return err
		}
		if b.Blk.Filename == "" {
			return errors.New("buffer is not assigned to a block")
		}
		if err := b.fm.Write(b.Blk, b.Contents); err != nil {
			return err
		}
		b.Txnum = -1
	}
	return nil
}

// Pin increases the buffer's pin count
func (b *Buffer) Pin() {
	b.pins += 1
}

// Unpin decreases the buffer's pin count
func (b *Buffer) Unpin() {
	b.pins -= 1
}
