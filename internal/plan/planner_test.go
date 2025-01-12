package plan_test

import (
	"fmt"
	"os"
	"simpledb/internal/server"
	"testing"

	"math/rand"
)

func TestPlanner1(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("plannertest1")
	})

	db, err := server.NewSimpleDB("plannertest1")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tx, err := db.NewTx()
	if err != nil {
		t.Fatalf("Failed to create transaction: %v", err)
	}

	cmd := "create table T1(A int, B varchar(9))"
	_, err = db.Planner.ExecuteUpdate(cmd, tx)
	if err != nil {
		t.Fatalf("Failed to execute update: %v", err)
	}

	n := 200
	t.Logf("Inserting %d random records into T1", n)
	for i := 0; i < n; i++ {
		a := rand.Intn(50)
		b := fmt.Sprintf("rec%d", a)
		cmd := fmt.Sprintf("insert into T1(A, B) values(%d, '%s')", a, b)
		_, err = db.Planner.ExecuteUpdate(cmd, tx)
		if err != nil {
			t.Fatalf("Failed to execute update: %v", err)
		}
	}

	qry := "select B from T1 where A=10"
	plan, err := db.Planner.CreateQueryPlan(qry, tx)
	if err != nil {
		t.Fatalf("Failed to create query plan: %v", err)
	}
	scan, err := plan.Open()
	if err != nil {
		t.Fatalf("Failed to open query plan: %v", err)
	}
	count := 0
	for scan.Next() {
		count++
		bval, err := scan.GetVal("B")
		if err != nil {
			t.Fatalf("Failed to get value: %v", err)
		}
		t.Logf("record %d: %v", count, bval)
	}
	scan.Close()
	t.Logf("Found %d records", count)

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}

func TestPlanner2(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("plannertest2")
	})

	db, err := server.NewSimpleDB("plannertest2")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tx, err := db.NewTx()
	if err != nil {
		t.Fatalf("Failed to create transaction: %v", err)
	}

	cmd := "create table T1(A int, B varchar(9))"
	_, err = db.Planner.ExecuteUpdate(cmd, tx)
	if err != nil {
		t.Fatalf("Failed to execute update: %v", err)
	}

	n := 200
	t.Logf("Inserting %d random records into T1", n)
	for i := 0; i < n; i++ {
		a := i
		b := fmt.Sprintf("bbb%d", a)
		cmd := fmt.Sprintf("insert into T1(A, B) values(%d, '%s')", a, b)
		_, err = db.Planner.ExecuteUpdate(cmd, tx)
		if err != nil {
			t.Fatalf("Failed to execute update: %v", err)
		}
	}

	cmd = "create table T2(C int, D varchar(9))"
	_, err = db.Planner.ExecuteUpdate(cmd, tx)
	if err != nil {
		t.Fatalf("Failed to execute update: %v", err)
	}

	t.Logf("Inserting %d random records into T2", n)
	for i := 0; i < n; i++ {
		c := n - i - 1
		d := fmt.Sprintf("ddd%d", c)
		cmd := fmt.Sprintf("insert into T2(C, D) values(%d, '%s')", c, d)
		_, err = db.Planner.ExecuteUpdate(cmd, tx)
		if err != nil {
			t.Fatalf("Failed to execute update: %v", err)
		}
	}

	qry := "select B, D from T1, T2 where A=C"
	plan, err := db.Planner.CreateQueryPlan(qry, tx)
	if err != nil {
		t.Fatalf("Failed to create query plan: %v", err)
	}
	scan, err := plan.Open()
	if err != nil {
		t.Fatalf("Failed to open query plan: %v", err)
	}
	count := 0
	for scan.Next() {
		count++
		bval, err := scan.GetVal("B")
		if err != nil {
			t.Fatalf("Failed to get value: %v", err)
		}
		dval, err := scan.GetVal("D")
		if err != nil {
			t.Fatalf("Failed to get value: %v", err)
		}
		t.Logf("record %d: %v, %v", count, bval, dval)
	}
	scan.Close()
	t.Logf("Found %d records", count)

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}
