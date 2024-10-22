package tx

import (
	"os"
	"simpledb/internal/file"
	"testing"
)

func TestTransaction(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("txtest")
	})

	db = createPartialDB(t, "txtest", 400, 8)
	defer closePartialDB(db)

	tx1 := newTx(t, db.fm, db.lm, db.bm)
	blk := file.NewBlockID("testfile", 1)
	if err := tx1.Pin(blk); err != nil {
		t.Fatalf("Failed to pin block: %v", err)
	}

	// The block initially contains unknown bytes, so don't log those values here.
	if err := tx1.SetInt(blk, 80, 1, false); err != nil {
		t.Fatalf("Failed to set int: %v", err)
	}
	if err := tx1.SetString(blk, 40, "one", false); err != nil {
		t.Fatalf("Failed to set string: %v", err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	tx2 := newTx(t, db.fm, db.lm, db.bm)
	if err := tx2.Pin(blk); err != nil {
		t.Fatalf("Failed to pin block: %v", err)
	}
	ival, err := tx2.GetInt(blk, 80)
	if err != nil {
		t.Fatalf("Failed to get int: %v", err)
	}
	if ival != 1 {
		t.Fatalf("Expected int value 1, got %d", ival)
	}
	sval, err := tx2.GetString(blk, 40)
	if err != nil {
		t.Fatalf("Failed to get string: %v", err)
	}
	if sval != "one" {
		t.Fatalf("Expected string value 'one', got %s", sval)
	}
	newival := ival + 1
	newsval := sval + "!"
	if err := tx2.SetInt(blk, 80, newival, true); err != nil {
		t.Fatalf("Failed to set int: %v", err)
	}
	if err := tx2.SetString(blk, 40, newsval, true); err != nil {
		t.Fatalf("Failed to set string: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	tx3 := newTx(t, db.fm, db.lm, db.bm)
	if err := tx3.Pin(blk); err != nil {
		t.Fatalf("Failed to pin block: %v", err)
	}
	ival, err = tx3.GetInt(blk, 80)
	if err != nil {
		t.Fatalf("Failed to get int: %v", err)
	}
	if ival != newival {
		t.Fatalf("Expected int value %d, got %d", newival, ival)
	}
	sval, err = tx3.GetString(blk, 40)
	if err != nil {
		t.Fatalf("Failed to get string: %v", err)
	}
	if sval != newsval {
		t.Fatalf("Expected string value %s, got %s", newsval, sval)
	}
	if err := tx3.SetInt(blk, 80, 9999, true); err != nil {
		t.Fatalf("Failed to set int: %v", err)
	}
	// read the pre-rollback value
	ival, err = tx3.GetInt(blk, 80)
	if err != nil {
		t.Fatalf("Failed to get int: %v", err)
	}
	if ival != 9999 {
		t.Fatalf("Expected int value %d, got %d", 9999, ival)
	}
	if err := tx3.Rollback(); err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	tx4 := newTx(t, db.fm, db.lm, db.bm)
	if err := tx4.Pin(blk); err != nil {
		t.Fatalf("Failed to pin block: %v", err)
	}
	// read the post-rollback value
	ival, err = tx4.GetInt(blk, 80)
	if err != nil {
		t.Fatalf("Failed to get int: %v", err)
	}
	if ival != newival {
		t.Fatalf("Expected int value %d, got %d", newival, ival)
	}
}
