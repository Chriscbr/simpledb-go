package metadata_test

import (
	"os"
	"simpledb/internal/metadata"
	"simpledb/internal/record"
	"simpledb/internal/server"
	"testing"
)

func TestTableMgr(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("tblmgrtest")
	})

	db, err := server.NewSimpleDBWithConfig("tblmgrtest", 400, 8)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tx, err := db.NewTx()
	if err != nil {
		t.Fatalf("Failed to create transaction: %v", err)
	}

	tm, err := metadata.NewTableMgr(true, tx)
	if err != nil {
		t.Fatalf("Failed to create table manager: %v", err)
	}

	sch := record.NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	if err := tm.CreateTable("MyTable", sch, tx); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	layout, err := tm.GetLayout("MyTable", tx)
	if err != nil {
		t.Fatalf("Failed to get layout: %v", err)
	}

	size := layout.SlotSize
	// 8 bytes for int (4 byte header + 4 byte data)
	// 13 bytes for string (4 byte header + 9 byte data)
	if size != 21 {
		t.Errorf("Expected slot size %d, got %d", 21, size)
	}

	sch2 := layout.Schema
	for _, fldname := range sch2.Fields {
		if fldname == "A" {
			fldtype := sch2.Type(fldname)
			if fldtype != record.Integer {
				t.Errorf("Field %s: expected type %v, got %v", fldname, record.Integer, fldtype)
			}
		} else if fldname == "B" {
			fldtype := sch2.Type(fldname)
			if fldtype != record.String {
				t.Errorf("Field %s: expected type %v, got %v", fldname, record.String, fldtype)
			}
			fldlen := sch2.Length(fldname)
			if fldlen != 9 {
				t.Errorf("Field %s: expected length %d, got %d", fldname, 9, fldlen)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}
