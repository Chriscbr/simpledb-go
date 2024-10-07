package simpledb

import (
	"fmt"
	"log"
	"os"
	"testing"
)

func TestFileMgr(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("filetest")
	})

	db, err := NewSimpleDB("filetest", 400)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fm := db.fm
	blk := NewBlockId("testfile", 2)
	pos1 := 0

	p1 := NewPage(fm.blocksize)
	p1.SetString(pos1, "abcdefghijklm")
	size := MaxLength(len("abcdefghijklm"))
	pos2 := pos1 + size
	p1.SetInt(pos2, 345)

	if err := fm.Write(blk, p1); err != nil {
		log.Fatal(err)
	}

	p2 := NewPage(fm.blocksize)
	if err := fm.Read(blk, p2); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("offset %d contains %d\n", pos2, p2.GetInt(pos2))
	fmt.Printf("offset %d contains %s\n", pos1, p2.GetString(pos1))
}
