package record_test

import (
	"fmt"
	"math/rand"
	"os"
	"simpledb/internal/record"
	"simpledb/internal/server"
	"testing"
)

func TestTableScan(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("tabletest")
	})

	db, err := server.NewSimpleDB("tabletest", 400, 8)
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

	t.Log("Filling the table with 50 random records")
	ts, err := record.NewTableScan(tx, "T", layout)
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
		t.Logf("Inserting into slot %s: {%d, rec%d}", ts.GetRid(), n, n)
	}

	t.Log("Deleting records whose A-values are less than 25")
	count := 0
	if err := ts.BeforeFirst(); err != nil {
		t.Fatal(err)
	}
	for ts.Next() {
		a, err := ts.GetInt("A")
		if err != nil {
			t.Fatal(err)
		}
		b, err := ts.GetString("B")
		if err != nil {
			t.Fatal(err)
		}
		if a < 25 {
			t.Logf("Deleting slot %s: {%d, %s}", ts.GetRid(), a, b)
			if err := ts.Delete(); err != nil {
				t.Fatal(err)
			}
			count++
		}
	}
	t.Logf("Deleted %d records", count)

	t.Log("Printing the remaining records")
	if err := ts.BeforeFirst(); err != nil {
		t.Fatal(err)
	}
	for ts.Next() {
		a, err := ts.GetInt("A")
		if err != nil {
			t.Fatal(err)
		}
		b, err := ts.GetString("B")
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("Slot %s: {%d, %s}", ts.GetRid(), a, b)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}
