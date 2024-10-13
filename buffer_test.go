package simpledb

import (
	"os"
	"testing"
)

func TestBuffer(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("buffertest")
	})

	db, err := NewSimpleDB("buffertest", 400, 3) // only 3 buffers
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	bm := db.BufferMgr
	b1, err := bm.Pin(NewBlockId("testfile", 1))
	if err != nil {
		t.Fatal(err)
	}
	p := b1.Contents
	n := p.GetInt(80)
	p.SetInt(80, n+1)
	b1.SetModified(1, 0) // placeholder values
	t.Logf("The new value is %v\n", n+1)
	bm.Unpin(b1)

	// One of these pins will flush b1 to disk:
	b2, err := bm.Pin(NewBlockId("testfile", 2))
	if err != nil {
		t.Fatal(err)
	}
	_, err = bm.Pin(NewBlockId("testfile", 3))
	if err != nil {
		t.Fatal(err)
	}
	_, err = bm.Pin(NewBlockId("testfile", 4))
	if err != nil {
		t.Fatal(err)
	}

	bm.Unpin(b2)
	b2, err = bm.Pin(NewBlockId("testfile", 1))
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

	db, err := NewSimpleDB("buffermgrtest", 400, 3) // only 3 buffers
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	bm := db.BufferMgr

	bs := make([]*Buffer, 6)

	bs[0], err = bm.Pin(NewBlockId("testfile", 0))
	if err != nil {
		t.Fatal(err)
	}
	bs[1], err = bm.Pin(NewBlockId("testfile", 1))
	if err != nil {
		t.Fatal(err)
	}
	bs[2], err = bm.Pin(NewBlockId("testfile", 2))
	if err != nil {
		t.Fatal(err)
	}

	bm.Unpin(bs[1])
	bs[1] = nil

	bs[3], err = bm.Pin(NewBlockId("testfile", 0)) // block 0 pinned twice
	if err != nil {
		t.Fatal(err)
	}
	bs[4], err = bm.Pin(NewBlockId("testfile", 1)) // block 1 repinned
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Available buffers: %d", bm.Available())

	t.Log("Attempting to pin block 3...")
	_, err = bm.Pin(NewBlockId("testfile", 3)) // will not work; no buffers left
	if err == nil {
		t.Fatal("Expected BufferAbortError, but got nil")
	}
	if _, ok := err.(*BufferAbortError); !ok {
		t.Fatalf("Expected BufferAbortError, but got %v", err)
	}
	t.Log("Exception: No available buffers")

	bm.Unpin(bs[2])
	bs[2] = nil

	bs[5], err = bm.Pin(NewBlockId("testfile", 3)) // now this works
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Final Buffer Allocation:")
	for i, b := range bs {
		if b != nil {
			t.Logf("bs[%d] pinned to block %v", i, b.Blk)
		}
	}
}
