package record_test

import (
	"fmt"
	"math/rand"
	"os"
	"simpledb/internal/record"
	"simpledb/internal/server"
	"testing"
)

func TestRecord(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("recordtest")
	})

	db, err := server.NewSimpleDB("recordtest", 400, 8)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tx, err := db.NewTx()
	if err != nil {
		t.Fatalf("Failed to create transaction: %v", err)
	}

	sch := record.NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	layout := record.NewLayout(sch)
	expectedOffsets := map[string]int{
		"A": 4,
		"B": 8,
	}
	for _, fldname := range layout.Schema.Fields {
		offset := layout.Offset(fldname)
		expected := expectedOffsets[fldname]
		if offset != expected {
			t.Errorf("Field %s: expected offset %d, got offset %d",
				fldname, expected, offset)
		}
	}

	blk, err := tx.Append("testfile")
	if err != nil {
		t.Fatalf("Failed to append to file: %v", err)
	}
	tx.Pin(blk)

	rp := record.NewRecordPage(tx, blk, layout)
	err = rp.Format()
	if err != nil {
		t.Fatalf("Failed to format record page: %v", err)
	}

	t.Log("Filling the page with random records")
	slot := rp.InsertAfter(-1)
	for slot >= 0 {
		n := rand.Intn(50)
		rp.SetInt(slot, "A", int32(n))
		rp.SetString(slot, "B", fmt.Sprintf("rec%d", n))
		t.Logf("Inserting into slot %d: {%d, rec%d}", slot, n, n)
		slot = rp.InsertAfter(slot)
	}

	t.Log("Deleting records whose A-values are less than 25")
	count := 0
	slot = rp.NextAfter(-1)
	for slot >= 0 {
		a, err := rp.GetInt(slot, "A")
		if err != nil {
			t.Fatal(err)
		}
		b, err := rp.GetString(slot, "B")
		if err != nil {
			t.Fatal(err)
		}
		if a < 25 {
			t.Logf("Deleting slot %d: {%d, %s}", slot, a, b)
			rp.Delete(slot)
			count++
		}
		slot = rp.NextAfter(slot)
	}
	t.Logf("Deleted %d records", count)

	t.Log("Printing the remaining records")
	slot = rp.NextAfter(-1)
	for slot >= 0 {
		a, err := rp.GetInt(slot, "A")
		if err != nil {
			t.Fatal(err)
		}
		b, err := rp.GetString(slot, "B")
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("Slot %d: {%d, %s}", slot, a, b)
		slot = rp.NextAfter(slot)
	}
	tx.Unpin(blk)
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}
