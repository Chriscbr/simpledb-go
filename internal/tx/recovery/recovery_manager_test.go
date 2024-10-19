package recovery

// import (
// 	"fmt"
// 	"os"
// 	"simpledb/internal/buffer"
// 	"simpledb/internal/file"
// 	"simpledb/internal/log"
// 	"simpledb/internal/server"
// 	"simpledb/internal/tx"
// 	"testing"
// )

// var (
// 	fm   *file.FileMgr
// 	bm   *buffer.BufferMgr
// 	db   *server.SimpleDB
// 	blk0 file.BlockID
// 	blk1 file.BlockID
// )

// func TestRecovery(t *testing.T) {
// 	t.Cleanup(func() {
// 		os.RemoveAll("recoverytest")
// 	})

// 	db, err := server.NewSimpleDB("recoverytest", 400, 8)
// 	if err != nil {
// 		t.Fatalf("Failed to create SimpleDB: %v", err)
// 	}
// 	defer db.Close()

// 	fm = db.FileMgr
// 	bm = db.BufferMgr
// 	blk0 = file.NewBlockID("testfile", 0)
// 	blk1 = file.NewBlockID("testfile", 1)

// 	length, err := fm.Length("testfile")
// 	if err != nil {
// 		t.Fatalf("Failed to get file length: %v", err)
// 	}

// 	if length == 0 {
// 		initialize(t)
// 		modify(t)
// 	} else {
// 		recover(t)
// 	}
// }

// func initialize(t *testing.T) {
// 	tx1 := newTransaction(t)
// 	tx2 := newTransaction(t)
// 	defer tx1.Commit()
// 	defer tx2.Commit()

// 	err := tx1.Pin(blk0)
// 	if err != nil {
// 		t.Fatalf("Failed to pin block: %v", err)
// 	}
// 	err = tx2.Pin(blk1)
// 	if err != nil {
// 		t.Fatalf("Failed to pin block: %v", err)
// 	}

// 	pos := 0
// 	for i := 0; i < 6; i++ {
// 		tx1.SetInt(blk0, pos, pos, false)
// 		tx2.SetInt(blk1, pos, pos, false)
// 		pos += 4 // Integer size in bytes
// 	}

// 	tx1.SetString(blk0, 30, "abc", false)
// 	tx2.SetString(blk1, 30, "def", false)

// 	printValues(t, "After Initialization:")
// }

// func modify(t *testing.T) {
// 	tx3 := newTransaction(t)
// 	tx4 := newTransaction(t)
// 	defer tx3.Rollback()
// 	// tx4 is intentionally not committed or rolled back

// 	err := tx3.Pin(blk0)
// 	if err != nil {
// 		t.Fatalf("Failed to pin block: %v", err)
// 	}
// 	err = tx4.Pin(blk1)
// 	if err != nil {
// 		t.Fatalf("Failed to pin block: %v", err)
// 	}

// 	pos := 0
// 	for i := 0; i < 6; i++ {
// 		tx3.SetInt(blk0, pos, pos+100, true)
// 		tx4.SetInt(blk1, pos, pos+100, true)
// 		pos += 4 // Integer size in bytes
// 	}

// 	tx3.SetString(blk0, 30, "uvw", true)
// 	tx4.SetString(blk1, 30, "xyz", true)

// 	err = bm.FlushAll(3)
// 	if err != nil {
// 		t.Fatalf("Failed to flush buffers: %v", err)
// 	}
// 	err = bm.FlushAll(4)
// 	if err != nil {
// 		t.Fatalf("Failed to flush buffers: %v", err)
// 	}

// 	printValues(t, "After modification:")

// 	tx3.Rollback()
// 	printValues(t, "After rollback:")
// }

// func recover(t *testing.T) {
// 	tx := newTransaction(t)
// 	defer tx.Commit()

// 	tx.Recover()
// 	printValues(t, "After recovery:")
// }

// func printValues(t *testing.T, msg string) {
// 	t.Log(msg)

// 	p0 := file.NewPage(fm.BlockSize)
// 	p1 := file.NewPage(fm.BlockSize)

// 	err := fm.Read(blk0, p0)
// 	if err != nil {
// 		t.Fatalf("Failed to read block: %v", err)
// 	}
// 	err = fm.Read(blk1, p1)
// 	if err != nil {
// 		t.Fatalf("Failed to read block: %v", err)
// 	}

// 	pos := 0
// 	var values []interface{}
// 	for i := 0; i < 6; i++ {
// 		values = append(values, p0.GetInt(pos), p1.GetInt(pos))
// 		pos += 4 // Integer size in bytes
// 	}
// 	values = append(values, p0.GetString(30), p1.GetString(30))

// 	t.Log(fmt.Sprintf("%v %v %v %v %v %v %v %v %v %v %v %v %v %v",
// 		values...))
// }

// func newTransaction(t *testing.T) *tx.Transaction {
// 	lm, err := log.NewLogMgr(fm, log.DefaultLogFile)
// 	if err != nil {
// 		t.Fatalf("Failed to create LogMgr: %v", err)
// 	}
// 	return tx.NewTransaction(fm, lm, bm)
// }
