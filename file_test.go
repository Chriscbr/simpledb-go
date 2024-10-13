package simpledb

import (
	"os"
	"testing"
)

func TestFileMgr(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("filetest")
	})

	db, err := NewSimpleDB("filetest", 400, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fm := db.FileMgr
	blk := NewBlockId("testfile", 2)
	pos1 := 0

	p1 := NewPage(fm.BlockSize)
	p1.SetString(pos1, "abcdefghijklm")
	size := MaxLength(len("abcdefghijklm"))
	pos2 := pos1 + size
	p1.SetInt(pos2, 345)

	if err := fm.Write(blk, p1); err != nil {
		t.Fatal(err)
	}

	p2 := NewPage(fm.BlockSize)
	if err := fm.Read(blk, p2); err != nil {
		t.Fatal(err)
	}

	t.Logf("offset %d contains %d\n", pos2, p2.GetInt(pos2))
	t.Logf("offset %d contains %s\n", pos1, p2.GetString(pos1))
}
