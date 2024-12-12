package buffer_test

import (
	"os"
	"simpledb/internal/buffer"
	"simpledb/internal/file"
	"simpledb/internal/server"
	"testing"
)

func TestBuffer(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("buffertest")
	})

	db, err := server.NewSimpleDBWithConfig("buffertest", 400, 3) // only 3 buffers
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	bm := db.BufferMgr
	b1, err := bm.Pin(file.NewBlockID("testfile", 1))
	if err != nil {
		t.Fatal(err)
	}
	p := b1.Contents
	n := p.GetInt(80)
	p.SetInt(80, n+1)
	b1.SetModified(1, 0) // placeholder values
	if got := p.GetInt(80); got != n+1 {
		t.Errorf("Expected new value to be %d, but got %d", n+1, got)
	}
	bm.Unpin(b1)

	// One of these pins will flush b1 to disk:
	b2, err := bm.Pin(file.NewBlockID("testfile", 2))
	if err != nil {
		t.Fatal(err)
	}
	_, err = bm.Pin(file.NewBlockID("testfile", 3))
	if err != nil {
		t.Fatal(err)
	}
	_, err = bm.Pin(file.NewBlockID("testfile", 4))
	if err != nil {
		t.Fatal(err)
	}

	bm.Unpin(b2)
	b2, err = bm.Pin(file.NewBlockID("testfile", 1))
	if err != nil {
		t.Fatal(err)
	}

	// This modification won't get written to disk
	p2 := b2.Contents
	p2.SetInt(80, 9999)
	b2.SetModified(1, 0)
}

func TestBufferMgr(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("buffermgrtest")
	})

	db, err := server.NewSimpleDBWithConfig("buffermgrtest", 400, 3) // only 3 buffers
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	bm := db.BufferMgr
	bs := make([]*buffer.Buffer, 6)
	pinBlock := func(index int, blockNum int) {
		b, err := bm.Pin(file.NewBlockID("testfile", blockNum))
		if err != nil {
			t.Fatal(err)
		}
		bs[index] = b
	}

	pinBlock(0, 0)
	pinBlock(1, 1)
	pinBlock(2, 2)

	bm.Unpin(bs[1])
	bs[1] = nil

	pinBlock(3, 0) // block 0 pinned twice
	pinBlock(4, 1) // block 1 repinned

	if bm.Available() != 0 {
		t.Errorf("Expected 0 available buffers, but got %d", bm.Available())
	}

	// Attempting to pin block 3 will not work; no buffers left
	_, err = bm.Pin(file.NewBlockID("testfile", 3)) // will not work; no buffers left
	if err == nil {
		t.Fatal("Expected BufferAbortError, but got nil")
	}
	if _, ok := err.(*buffer.BufferAbortError); !ok {
		t.Fatalf("Expected BufferAbortError, but got %v", err)
	}

	bm.Unpin(bs[2])
	bs[2] = nil

	pinBlock(5, 3) // now this works

	if bs[0].Blk != file.NewBlockID("testfile", 0) {
		t.Errorf("bs[0] should be pinned to block [file testfile, block 0], but got %v", bs[0].Blk)
	}
	if bs[3].Blk != file.NewBlockID("testfile", 0) {
		t.Errorf("bs[3] should be pinned to block [file testfile, block 0], but got %v", bs[3].Blk)
	}
	if bs[4].Blk != file.NewBlockID("testfile", 1) {
		t.Errorf("bs[4] should be pinned to block [file testfile, block 1], but got %v", bs[4].Blk)
	}
	if bs[5].Blk != file.NewBlockID("testfile", 3) {
		t.Errorf("bs[5] should be pinned to block [file testfile, block 3], but got %v", bs[5].Blk)
	}
	if bs[1] != nil {
		t.Errorf("bs[1] should be nil, but got %v", bs[1])
	}
	if bs[2] != nil {
		t.Errorf("bs[2] should be nil, but got %v", bs[2])
	}
}

func TestBufferFile(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("bufferfiletest")
	})

	db, err := server.NewSimpleDBWithConfig("bufferfiletest", 400, 3) // only 3 buffers
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	bm := db.BufferMgr
	blk := file.NewBlockID("testfile", 2)
	pos1 := 88

	b1, err := bm.Pin(blk)
	if err != nil {
		t.Fatal(err)
	}
	p1 := b1.Contents
	p1.SetString(pos1, "abcdefghijklm")
	size := file.MaxLength(len("abcdefghijklm"))
	pos2 := pos1 + size
	p1.SetInt(pos2, 345)
	b1.SetModified(1, 0)
	bm.Unpin(b1)

	b2, err := bm.Pin(blk)
	if err != nil {
		t.Fatal(err)
	}
	p2 := b2.Contents
	if p2.GetInt(pos2) != 345 {
		t.Errorf("Expected offset %v to contain 345, but got %v", pos2, p2.GetInt(pos2))
	}
	if p2.GetString(pos1) != "abcdefghijklm" {
		t.Errorf("Expected offset %v to contain 'abcdefghijklm', but got %v", pos1, p2.GetString(pos1))
	}
	bm.Unpin(b2)
}
