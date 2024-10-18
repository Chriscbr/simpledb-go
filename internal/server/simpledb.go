package server

import (
	"simpledb/internal/buffer"
	"simpledb/internal/file"
	"simpledb/internal/log"
)

type SimpleDB struct {
	FileMgr   *file.FileMgr
	LogMgr    *log.LogMgr
	BufferMgr *buffer.BufferMgr
}

// NewSimpleDB creates a new SimpleDB instance with the given directory name and blocksize.
// The instance should be closed by calling Close() when it's no longer needed.
func NewSimpleDB(dirname string, blocksize int, numbufs int) (*SimpleDB, error) {
	fm, err := file.NewFileMgr(dirname, blocksize)
	if err != nil {
		return nil, err
	}

	lm, err := log.NewLogMgr(fm, log.DefaultLogFile)
	if err != nil {
		return nil, err
	}

	bm, err := buffer.NewBufferMgr(fm, lm, numbufs)
	if err != nil {
		return nil, err
	}

	db := &SimpleDB{fm, lm, bm}
	return db, nil
}

// Close closes the SimpleDB instance
func (db *SimpleDB) Close() {
	db.FileMgr.Close()
}
