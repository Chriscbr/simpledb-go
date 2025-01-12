package server

import (
	"fmt"
	"simpledb/internal/buffer"
	"simpledb/internal/file"
	"simpledb/internal/log"
	"simpledb/internal/metadata"
	"simpledb/internal/plan"
	"simpledb/internal/tx"
	"simpledb/internal/tx/concurrency"
)

const (
	DefaultBlockSize  = 400
	DefaultBufferSize = 8
)

type SimpleDB struct {
	FileMgr     *file.FileMgr
	LogMgr      *log.LogMgr
	BufferMgr   *buffer.BufferMgr
	LockTable   *concurrency.LockTable
	MetadataMgr *metadata.MetadataMgr
	Planner     *plan.Planner
}

// NewSimpleDBWithConfig creates a new SimpleDB instance with the given directory name and blocksize.
// The instance should be closed by calling Close() when it's no longer needed.
func NewSimpleDBWithConfig(dirname string, blocksize int, numbufs int) (*SimpleDB, error) {
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

	lt := concurrency.NewLockTable()

	db := &SimpleDB{fm, lm, bm, lt, nil, nil}
	return db, nil
}

// NewSimpleDB creates a new SimpleDB instance with a default configuration.
// It also initializes the metadata tables.
func NewSimpleDB(dirname string) (*SimpleDB, error) {
	db, err := NewSimpleDBWithConfig(dirname, DefaultBlockSize, DefaultBufferSize)
	if err != nil {
		return nil, err
	}
	tx, err := db.NewTx()
	if err != nil {
		return nil, err
	}
	isNew := db.FileMgr.IsNew
	if isNew {
		fmt.Println("creating new database")
	} else {
		fmt.Println("recovering existing database")
		err := tx.Recover()
		if err != nil {
			return nil, err
		}
	}
	mdm, err := metadata.NewMetadataMgr(isNew, tx)
	if err != nil {
		return nil, err
	}
	db.MetadataMgr = mdm
	db.Planner = plan.NewPlanner(plan.NewBasicQueryPlanner(mdm), plan.NewBasicUpdatePlanner(mdm))
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return db, nil
}

func (db *SimpleDB) NewTx() (*tx.Transaction, error) {
	return tx.NewTransaction(db.FileMgr, db.LogMgr, db.BufferMgr, db.LockTable)
}

// Close closes the SimpleDB instance
func (db *SimpleDB) Close() {
	db.FileMgr.Close()
}
