package query_test

import (
	"fmt"
	"math/rand"
	"os"
	"simpledb/internal/query"
	"simpledb/internal/record"
	"simpledb/internal/server"
	"testing"
)

func TestScan1(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("scantest1")
	})

	db, err := server.NewSimpleDB("scantest1")
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
	s1, err := record.NewTableScan(tx, "T", layout)
	if err != nil {
		t.Fatalf("Failed to create table scan: %v", err)
	}

	if err := s1.BeforeFirst(); err != nil {
		t.Fatalf("Failed to move to first record: %v", err)
	}
	n := 200
	t.Logf("Inserting %d random records into table T", n)
	for i := 0; i < n; i++ {
		if err := s1.Insert(); err != nil {
			t.Fatal(err)
		}
		k := rand.Intn(50)
		if err := s1.SetInt("A", int32(k)); err != nil {
			t.Fatal(err)
		}
		if err := s1.SetString("B", fmt.Sprintf("rec%d", k)); err != nil {
			t.Fatal(err)
		}
	}
	s1.Close()

	s2, err := record.NewTableScan(tx, "T", layout)
	if err != nil {
		t.Fatalf("Failed to create table scan: %v", err)
	}

	// selecting all records where A=10
	c := record.NewIntConstant(10)
	term := query.NewTerm(query.NewFieldExpression("A"), query.NewConstantExpression(c))
	pred := query.NewPredicate([]*query.Term{term})
	t.Logf("The predicate is: %v", pred)

	s3 := query.NewSelectScan(s2, pred)
	fields := []string{"B"}
	s4 := query.NewProjectScan(s3, fields)
	for s4.Next() {
		b, err := s4.GetString("B")
		if err != nil {
			t.Fatalf("Failed to get value: %v", err)
		}
		t.Logf("Selected record: {B: %v}", b)
	}
	s4.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}
