package file

import (
	"os"
	"testing"
)

func TestFileMgr(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("filetest")
	})

	fm, err := NewFileMgr("filetest", 400)
	if err != nil {
		t.Fatal(err)
	}
	defer fm.Close()

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

	if p2.GetInt(pos2) != 345 {
		t.Errorf("Expected offset %d to contain 345, but got %d", pos2, p2.GetInt(pos2))
	}
	if p2.GetString(pos1) != "abcdefghijklm" {
		t.Errorf("Expected offset %d to contain 'abcdefghijklm', but got %s", pos1, p2.GetString(pos1))
	}
}
