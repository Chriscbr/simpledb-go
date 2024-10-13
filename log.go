package simpledb

import (
	"iter"
	"sync"
)

// The log manager, which is responsible for writing log records into a file.
// The tail of the log is kept in a buffer, which is flushed to disk when needed.
type LogMgr struct {
	fm           *FileMgr
	logfile      string
	logpage      *Page
	currentblk   *BlockId
	latestLSN    int
	lastSavedLSN int
	mu           sync.Mutex
}

// Creates a new LogMgr instance with the specified file manager and logfile.
func NewLogMgr(fm *FileMgr, logfile string) (*LogMgr, error) {
	buf := make([]byte, fm.BlockSize)
	logpage := NewPageFromBytes(buf)
	logsize, err := fm.Length(logfile)
	if err != nil {
		return nil, err
	}

	lm := &LogMgr{
		fm:           fm,
		logfile:      logfile,
		logpage:      logpage,
		currentblk:   nil,
		latestLSN:    0,
		lastSavedLSN: 0,
	}

	if logsize == 0 {
		if err := lm.appendNewBlock(); err != nil {
			return nil, err
		}
	} else {
		lm.currentblk = NewBlockId(logfile, logsize-1)
	}

	return lm, nil
}

// Ensures the log record corresponding to the specified LSN has been written to disk.
// All earlier log records will also be written to disk.
func (lm *LogMgr) Flush(lsn int) error {
	if lsn >= lm.lastSavedLSN {
		return lm.forceFlush()
	}
	return nil
}

// Appends a log record to the log buffer.
// The record consists of an arbitrary array of bytes.
// Log records are written right to left in the buffer.
// The size of the record is written before the bytes.
// The beginning of the buffer contains the location
// of the last-written record (the "boundary").
// Storing the records backwards makes it easy to read them
// in reverse order.
func (lm *LogMgr) Append(logrec []byte) (int, error) {
	// Prevent two threads from mutating the same page
	lm.mu.Lock()
	defer lm.mu.Unlock()

	boundary := lm.logpage.GetInt(0)
	recsize := len(logrec)
	bytesneeded := int32(4 + recsize)
	if boundary-bytesneeded < 4 { // the log record doesn't fit,
		lm.forceFlush() // so move to the next block.
		if err := lm.appendNewBlock(); err != nil {
			return 0, err
		}
		boundary = lm.logpage.GetInt(0)
	}
	recpos := boundary - bytesneeded

	lm.logpage.SetBytes(int(recpos), logrec)
	lm.logpage.SetInt(0, recpos) // the new boundary
	lm.latestLSN += 1
	return lm.latestLSN, nil
}

// Returns the records of the log file in reverse order.
func (lm *LogMgr) All() iter.Seq2[[]byte, error] {
	err := lm.forceFlush()
	if err != nil {
		// we couldn't flush, so return a dummy iterator with the error
		return func(yield func([]byte, error) bool) {
			yield(nil, err)
		}
	}

	li, err := NewLogIterator(lm.fm, lm.currentblk)
	if err != nil {
		// we couldn't flush, so return a dummy iterator with the error
		return func(yield func([]byte, error) bool) {
			yield(nil, err)
		}
	}

	return func(yield func([]byte, error) bool) {
		for {
			if !li.HasNext() {
				return
			}
			v, err := li.Next()
			if !yield(v, err) {
				return
			}
		}
	}
}

func (lm *LogMgr) appendNewBlock() error {
	blk, err := lm.fm.Append(lm.logfile)
	if err != nil {
		return err
	}

	lm.logpage.SetInt(0, int32(lm.fm.BlockSize)) // TODO: handle int overflow better
	err = lm.fm.Write(blk, lm.logpage)
	if err != nil {
		return err
	}

	lm.currentblk = blk
	return nil
}

func (lm *LogMgr) forceFlush() error {
	err := lm.fm.Write(lm.currentblk, lm.logpage)
	if err != nil {
		return err
	}
	lm.lastSavedLSN = lm.latestLSN
	return nil
}

// Provides the ability to move through the records of the log file in reverse order.
type LogIterator struct {
	fm         *FileMgr
	blk        *BlockId
	p          *Page
	currentpos int
	boundary   int
}

// Creates an iterator for the records in the log file, positioned after the last log record.
func NewLogIterator(fm *FileMgr, blk *BlockId) (*LogIterator, error) {
	buf := make([]byte, fm.BlockSize)
	p := NewPageFromBytes(buf)
	li := &LogIterator{
		fm:         fm,
		blk:        blk,
		p:          p,
		currentpos: 0,
		boundary:   0,
	}
	if err := li.moveToBlock(blk); err != nil {
		return nil, err
	}
	return li, nil
}

// Determines if the current log record is the earliest record in the log file.
func (li *LogIterator) HasNext() bool {
	return li.currentpos < li.fm.BlockSize || li.blk.Blknum > 0
}

// Moves to the next log record in the block.
// If there are no more records in the block, then move to the previous block
// and return the log record from there.
func (li *LogIterator) Next() ([]byte, error) {
	if li.currentpos == li.fm.BlockSize {
		li.blk = NewBlockId(li.blk.Filename, li.blk.Blknum-1)
		err := li.moveToBlock(li.blk)
		if err != nil {
			return nil, err
		}
	}
	rec := li.p.GetBytes(li.currentpos)
	li.currentpos += 4 + len(rec)
	return rec, nil
}

// Moves to the specified log block and positions it at the first record
// in that block (i.e., the most recent one).
func (li *LogIterator) moveToBlock(blk *BlockId) error {
	err := li.fm.Read(blk, li.p)
	if err != nil {
		return err
	}

	li.boundary = int(li.p.GetInt(0)) // TODO: handle int overflow better
	li.currentpos = li.boundary
	return nil
}
