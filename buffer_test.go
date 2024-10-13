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
