package tx

import (
	"os"
	"simpledb/internal/buffer"
	"simpledb/internal/file"
	"simpledb/internal/log"
	"simpledb/internal/tx/concurrency"
	"testing"
)

var (
	db   *DB
	blk0 file.BlockID
	blk1 file.BlockID
)

func TestRecovery(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("recoverytest")
	})

	db = createPartialDB(t, "recoverytest", 400, 8)

	blk0 = file.NewBlockID("testfile", 0)
	blk1 = file.NewBlockID("testfile", 1)

	initialize(t)
	modify(t)

	closePartialDB(db)
	concurrency.ResetGlobalLockTableForTesting()
	db = createPartialDB(t, "recoverytest", 400, 8)

	recover(t)

	closePartialDB(db)
}

func initialize(t *testing.T) {
	tx1 := newTx(t)
	tx2 := newTx(t)

	err := tx1.Pin(blk0)
	if err != nil {
		t.Fatalf("Failed to pin block: %v", err)
	}
	err = tx2.Pin(blk1)
	if err != nil {
		t.Fatalf("Failed to pin block: %v", err)
	}

	pos := 0
	for i := 0; i < 6; i++ {
		err = tx1.SetInt(blk0, pos, int32(pos), false)
		if err != nil {
			t.Fatalf("Failed to set int: %v", err)
		}
		err = tx2.SetInt(blk1, pos, int32(pos), false)
		if err != nil {
			t.Fatalf("Failed to set int: %v", err)
		}
		pos += 4 // Integer size in bytes
	}

	err = tx1.SetString(blk0, 30, "abc", false)
	if err != nil {
		t.Fatalf("Failed to set string: %v", err)
	}
	err = tx2.SetString(blk1, 30, "def", false)
	if err != nil {
		t.Fatalf("Failed to set string: %v", err)
	}

	err = tx1.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	err = tx2.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	printValues(t, "After Initialization:")
}

// modify creates two new transactions, but does not commit them.
// The first is rolled back, and the second is left uncompleted.
func modify(t *testing.T) {
	tx3 := newTx(t)
	tx4 := newTx(t)

	err := tx3.Pin(blk0)
	if err != nil {
		t.Fatalf("Failed to pin block: %v", err)
	}
	err = tx4.Pin(blk1)
	if err != nil {
		t.Fatalf("Failed to pin block: %v", err)
	}

	pos := 0
	for i := 0; i < 6; i++ {
		err = tx3.SetInt(blk0, pos, int32(pos+100), true)
		if err != nil {
			t.Fatalf("Failed to set int: %v", err)
		}
		err = tx4.SetInt(blk1, pos, int32(pos+100), true)
		if err != nil {
			t.Fatalf("Failed to set int: %v", err)
		}
		pos += 4 // Integer size in bytes
	}

	err = tx3.SetString(blk0, 30, "uvw", true)
	if err != nil {
		t.Fatalf("Failed to set string: %v", err)
	}
	err = tx4.SetString(blk1, 30, "xyz", true)
	if err != nil {
		t.Fatalf("Failed to set string: %v", err)
	}

	err = db.bm.FlushAll(3)
	if err != nil {
		t.Fatalf("Failed to flush buffers: %v", err)
	}
	err = db.bm.FlushAll(4)
	if err != nil {
		t.Fatalf("Failed to flush buffers: %v", err)
	}

	printValues(t, "After modification:")

	err = tx3.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}
	printValues(t, "After rollback:")
	// tx4 is intentionally not committed or rolled back
	// so all of its changes should be undone during recovery
}

func recover(t *testing.T) {
	tx5 := newTx(t)

	err := tx5.Recover()
	if err != nil {
		t.Fatalf("Failed to recover transaction: %v", err)
	}
	printValues(t, "After recovery:")
}

func printValues(t *testing.T, msg string) {
	t.Log(msg)

	p0 := file.NewPage(db.fm.BlockSize)
	p1 := file.NewPage(db.fm.BlockSize)

	err := db.fm.Read(blk0, p0)
	if err != nil {
		t.Fatalf("Failed to read block: %v", err)
	}
	err = db.fm.Read(blk1, p1)
	if err != nil {
		t.Fatalf("Failed to read block: %v", err)
	}

	pos := 0
	var values []interface{}
	for i := 0; i < 6; i++ {
		values = append(values, p0.GetInt(pos), p1.GetInt(pos))
		pos += 4 // Integer size in bytes
	}
	values = append(values, p0.GetString(30), p1.GetString(30))

	t.Logf("%v %v %v %v %v %v %v %v %v %v %v %v %v %v",
		values...)
}

func newTx(t *testing.T) *Transaction {
	tx, err := NewTransaction(db.fm, db.lm, db.bm)
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
