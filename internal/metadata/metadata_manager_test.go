package metadata_test

import (
	"fmt"
	"math/rand"
	"os"
	"simpledb/internal/metadata"
	"simpledb/internal/record"
	"simpledb/internal/server"
	"testing"
)

func TestMetadataMgr(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("metadatamgrtest")
	})

	db, err := server.NewSimpleDB("metadatamgrtest", 400, 8)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tx, err := db.NewTx()
	if err != nil {
		t.Fatalf("Failed to create transaction: %v", err)
	}

	mdm, err := metadata.NewMetadataMgr(true, tx)
	if err != nil {
		t.Fatalf("Failed to create metadata manager: %v", err)
	}

	sch := record.NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)

	// Part 1: Table Metadata
	if err := mdm.CreateTable("MyTable", sch, tx); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	layout, err := mdm.GetLayout("MyTable", tx)
	if err != nil {
		t.Fatalf("Failed to get layout: %v", err)
	}

	t.Logf("MyTable has slot size %d", layout.SlotSize)
	t.Log("Its fields are:")
	for _, fldname := range layout.Schema.Fields {
		var typeStr string
		if layout.Schema.Type(fldname) == record.Integer {
			typeStr = "int"
		} else {
			strlen := layout.Schema.Length(fldname)
			typeStr = fmt.Sprintf("varchar(%d)", strlen)
		}
		t.Logf("%s: %s", fldname, typeStr)
	}

	// Part 2: Statistics Metadata
	ts, err := record.NewTableScan(tx, "MyTable", layout)
	if err != nil {
		t.Fatalf("Failed to create table scan: %v", err)
	}
	defer ts.Close()

	for i := 0; i < 50; i++ {
		if err := ts.Insert(); err != nil {
			t.Fatal(err)
		}
		n := rand.Intn(50)
		if err := ts.SetInt("A", int32(n)); err != nil {
			t.Fatal(err)
		}
		if err := ts.SetString("B", fmt.Sprintf("rec%d", n)); err != nil {
			t.Fatal(err)
		}
	}

	si, err := mdm.GetStatInfo("MyTable", layout, tx)
	if err != nil {
		t.Fatalf("Failed to get stat info: %v", err)
	}

	t.Logf("B(MyTable) = %d", si.BlocksAccessed)
	t.Logf("R(MyTable) = %d", si.RecordsOutput)
	t.Logf("V(MyTable,A) = %d", si.DistinctValues("A"))
	t.Logf("V(MyTable,B) = %d", si.DistinctValues("B"))

	// Part 3: View Metadata
	viewdef := "select B from MyTable where A = 1"
	if err := mdm.CreateView("viewA", viewdef, tx); err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	v, err := mdm.GetViewDef("viewA", tx)
	if err != nil {
		t.Fatalf("Failed to get view definition: %v", err)
	}
	t.Logf("View def = %s", v)

	// Part 4: Index Metadata
	if err := mdm.CreateIndex("indexA", "MyTable", "A", tx); err != nil {
		t.Fatalf("Failed to create index A: %v", err)
	}
	if err := mdm.CreateIndex("indexB", "MyTable", "B", tx); err != nil {
		t.Fatalf("Failed to create index B: %v", err)
	}

	idxmap, err := mdm.GetIndexInfo("MyTable", tx)
	if err != nil {
		t.Fatalf("Failed to get index info: %v", err)
	}

	if ii, ok := idxmap["A"]; ok {
		t.Logf("B(indexA) = %d", ii.BlocksAccessed())
		t.Logf("R(indexA) = %d", ii.RecordsOutput())
		t.Logf("V(indexA,A) = %d", ii.DistinctValues("A"))
		t.Logf("V(indexA,B) = %d", ii.DistinctValues("B"))
	}

	if ii, ok := idxmap["B"]; ok {
		t.Logf("B(indexB) = %d", ii.BlocksAccessed())
		t.Logf("R(indexB) = %d", ii.RecordsOutput())
		t.Logf("V(indexB,A) = %d", ii.DistinctValues("A"))
		t.Logf("V(indexB,B) = %d", ii.DistinctValues("B"))
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}
