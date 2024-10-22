package tx

import (
	"simpledb/internal/buffer"
	"simpledb/internal/file"
	"simpledb/internal/log"
	"testing"
)

func newTx(t *testing.T, fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr) *Transaction {
	tx, err := NewTransaction(fm, lm, bm)
	if err != nil {
		t.Fatalf("Failed to create Transaction: %v", err)
	}
	return tx
}

type DB struct {
	fm *file.FileMgr
	lm *log.LogMgr
	bm *buffer.BufferMgr
}

func createPartialDB(t *testing.T, dirname string, blocksize int, numbufs int) *DB {
	fm, err := file.NewFileMgr(dirname, blocksize)
	if err != nil {
		t.Fatal(err)
	}

	lm, err := log.NewLogMgr(fm, log.DefaultLogFile)
	if err != nil {
		t.Fatal(err)
	}

	bm, err := buffer.NewBufferMgr(fm, lm, numbufs)
	if err != nil {
		t.Fatal(err)
	}
	return &DB{fm, lm, bm}
}

func closePartialDB(db *DB) {
	db.fm.Close()
}
