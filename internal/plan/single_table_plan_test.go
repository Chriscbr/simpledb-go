package plan_test

import (
	"os"
	"simpledb/internal/plan"
	"simpledb/internal/query"
	"simpledb/internal/record"
	"simpledb/internal/server"
	"simpledb/internal/testutil"
	"testing"
)

func TestSingleTablePlan(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("singletableplantest")
	})

	db, err := server.NewSimpleDB("singletableplantest")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	testutil.SetupUniversityDB(t, db)

	tx, err := db.NewTx()
	if err != nil {
		t.Fatalf("Failed to create transaction: %v", err)
	}

	mdm := db.MetadataMgr

	// Student node
	p1, err := plan.NewTablePlan(tx, "student", mdm)
	if err != nil {
		t.Fatalf("Failed to create table plan: %v", err)
	}

	// Select node for "major = 20"
	term := query.NewTerm(query.NewFieldExpression("majorid"), query.NewConstantExpression(record.NewIntConstant(20)))
	pred := query.NewPredicate([]*query.Term{term})
	p2 := plan.NewSelectPlan(p1, pred)

	// Select node for "gradyear = 2020"
	term2 := query.NewTerm(query.NewFieldExpression("gradyear"), query.NewConstantExpression(record.NewIntConstant(2020)))
	pred2 := query.NewPredicate([]*query.Term{term2})
	p3 := plan.NewSelectPlan(p2, pred2)

	// Project node
	c := []string{"sname", "majorid", "gradyear"}
	p4, err := plan.NewProjectPlan(p3, c)
	if err != nil {
		t.Fatalf("Failed to create project plan: %v", err)
	}

	// Look at R(p) and B(p) for each plan p
	t.Logf("R(p1) = %d, B(p1) = %d", p1.RecordsOutput(), p1.BlocksAccessed())
	t.Logf("R(p2) = %d, B(p2) = %d", p2.RecordsOutput(), p2.BlocksAccessed())
	t.Logf("R(p3) = %d, B(p3) = %d", p3.RecordsOutput(), p3.BlocksAccessed())
	t.Logf("R(p4) = %d, B(p4) = %d", p4.RecordsOutput(), p4.BlocksAccessed())

	s, err := p4.Open()
	if err != nil {
		t.Fatalf("Failed to open plan: %v", err)
	}
	defer s.Close()

	for s.Next() {
		sname, err := s.GetString("sname")
		if err != nil {
			t.Fatalf("Failed to get sname: %v", err)
		}
		majorid, err := s.GetInt("majorid")
		if err != nil {
			t.Fatalf("Failed to get majorid: %v", err)
		}
		gradyear, err := s.GetInt("gradyear")
		if err != nil {
			t.Fatalf("Failed to get gradyear: %v", err)
		}
		t.Logf("Record: {%s, %d, %d}", sname, majorid, gradyear)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}
