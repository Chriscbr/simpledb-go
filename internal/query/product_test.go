package query_test

import (
	"fmt"
	"os"
	"simpledb/internal/query"
	"simpledb/internal/record"
	"simpledb/internal/server"
	"testing"
)

func TestProductScan(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("producttest")
	})

	db, err := server.NewSimpleDB("producttest")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tx, err := db.NewTx()
	if err != nil {
		t.Fatalf("Failed to create transaction: %v", err)
	}

	sch1 := record.NewSchema()
	sch1.AddIntField("A")
	sch1.AddStringField("B", 9)
	layout1 := record.NewLayout(sch1)
	ts1, err := record.NewTableScan(tx, "T1", layout1)
	if err != nil {
		t.Fatalf("Failed to create table scan: %v", err)
	}

	sch2 := record.NewSchema()
	sch2.AddIntField("C")
	sch2.AddStringField("D", 9)
	layout2 := record.NewLayout(sch2)
	ts2, err := record.NewTableScan(tx, "T2", layout2)
	if err != nil {
		t.Fatalf("Failed to create table scan: %v", err)
	}

	if err := ts1.BeforeFirst(); err != nil {
		t.Fatalf("Failed to move to first record: %v", err)
	}
	n := 5
	t.Logf("Inserting %d records into table T1", n)
	for i := 0; i < n; i++ {
		if err := ts1.Insert(); err != nil {
			t.Fatal(err)
		}
		if err := ts1.SetInt("A", int32(i)); err != nil {
			t.Fatal(err)
		}
		if err := ts1.SetString("B", fmt.Sprintf("aaa%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	ts1.Close()

	if err := ts2.BeforeFirst(); err != nil {
		t.Fatalf("Failed to move to first record: %v", err)
	}
	t.Logf("Inserting %d records into table T2", n)
	for i := 0; i < n; i++ {
		if err := ts2.Insert(); err != nil {
			t.Fatal(err)
		}
		if err := ts2.SetInt("C", int32(n-i-1)); err != nil {
			t.Fatal(err)
		}
		if err := ts2.SetString("D", fmt.Sprintf("bbb%d", n-i-1)); err != nil {
			t.Fatal(err)
		}
	}
	ts2.Close()

	s1, err := record.NewTableScan(tx, "T1", layout1)
	if err != nil {
		t.Fatalf("Failed to create table scan: %v", err)
	}
	s2, err := record.NewTableScan(tx, "T2", layout2)
	if err != nil {
		t.Fatalf("Failed to create table scan: %v", err)
	}
	s3, err := query.NewProductScan(s1, s2)
	if err != nil {
		t.Fatalf("Failed to create product scan: %v", err)
	}
	for s3.Next() {
		b, err := s3.GetString("B")
		if err != nil {
			t.Fatal(err)
		}
		d, err := s3.GetString("D")
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("B = %s, D = %s", b, d)
	}
	s3.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}
