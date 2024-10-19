package log

import (
	"iter"
	"sync"

	"simpledb/internal/file"
)

const DefaultLogFile = "simpledb.log"

// LogMgr is responsible for writing log records into a file.
// The tail of the log is kept in a buffer, which is flushed to disk when needed.
type LogMgr struct {
	fm           *file.FileMgr
	logfile      string
	logpage      *file.Page
	currentblk   file.BlockID
	latestLSN    int
	lastSavedLSN int
	mu           sync.Mutex
}

// NewLogMgr creates a new LogMgr instance with the specified file manager and logfile.
func NewLogMgr(fm *file.FileMgr, logfile string) (*LogMgr, error) {
	buf := make([]byte, fm.BlockSize)
	logpage := file.NewPageFromBytes(buf)
	logsize, err := fm.Length(logfile)
	if err != nil {
		return nil, err
	}

	lm := &LogMgr{
		fm:           fm,
		logfile:      logfile,
		logpage:      logpage,
		currentblk:   file.BlockID{},
		latestLSN:    0,
		lastSavedLSN: 0,
	}

	if logsize == 0 {
		if err := lm.appendNewBlock(); err != nil {
			return nil, err
		}
	} else {
		lm.currentblk = file.NewBlockID(logfile, logsize-1)
		if err := lm.fm.Read(lm.currentblk, lm.logpage); err != nil {
			return nil, err
		}
	}

	return lm, nil
}

// Flush ensures the log record corresponding to the specified LSN
// (log sequence number) has been written to disk.
// All earlier log records will also be written to disk.
func (lm *LogMgr) Flush(lsn int) error {
	if lsn >= lm.lastSavedLSN {
		return lm.forceFlush()
	}
	return nil
}

// Append appends a log record to the log buffer.
// The record consists of an arbitrary array of bytes.
// Log records are written right to left in the buffer.
// The size of the record is written before the bytes.
// The beginning of the buffer contains the location
// of the last-written record (the "boundary").
// Storing the records backwards makes it easy to read them
// in reverse order.
// Returns the LSN (log sequence number) of the final value.
func (lm *LogMgr) Append(logrec []byte) (int, error) {
	// Prevent two threads from mutating the same page or latestLSN
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

// All returns the records of the log file in reverse order.
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
		// we couldn't create the iterator, so return a dummy iterator with the error
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

// appendNewBlock appends a new block to the log file.
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

// forceFlush writes the log page to disk and updates the last saved LSN.
func (lm *LogMgr) forceFlush() error {
	err := lm.fm.Write(lm.currentblk, lm.logpage)
	if err != nil {
		return err
	}
	lm.lastSavedLSN = lm.latestLSN
	return nil
}
