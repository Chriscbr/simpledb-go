package testutil

import (
	"simpledb/internal/record"
	"simpledb/internal/server"
	"simpledb/internal/tx"
	"testing"
)

type student struct {
	sid      int32
	sname    string
	gradyear int32
	majorid  int32
}

var students = []student{
	{1, "joe", 2021, 10},
	{2, "amy", 2020, 20},
	{3, "max", 2022, 10},
	{4, "sue", 2022, 20},
	{5, "bob", 2020, 30},
	{6, "kim", 2020, 20},
	{8, "pat", 2019, 20},
	{9, "lee", 2021, 10},
}

type department struct {
	did   int32
	dname string
}

var departments = []department{
	{10, "compsci"},
	{20, "math"},
	{30, "drama"},
}

func SetupUniversityDB(t *testing.T, db *server.SimpleDB) {
	tx, err := db.NewTx()
	if err != nil {
		t.Fatalf("Failed to create transaction: %v", err)
	}
	defer tx.Commit()

	setupStudents(t, db, tx)
	setupDepartments(t, db, tx)
}

func setupStudents(t *testing.T, db *server.SimpleDB, tx *tx.Transaction) {
	ssch := record.NewSchema()
	ssch.AddIntField("sid")
	ssch.AddStringField("sname", 3)
	ssch.AddIntField("gradyear")
	ssch.AddIntField("majorid")
	slayout := record.NewLayout(ssch)
	if err := db.MetadataMgr.CreateTable("student", ssch, tx); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	sts, err := record.NewTableScan(tx, "student", slayout)
	if err != nil {
		t.Fatalf("Failed to create table scan: %v", err)
	}
	defer sts.Close()
	for _, s := range students {
		if err := sts.Insert(); err != nil {
			t.Fatalf("Failed to insert record: %v", err)
		}
		if err := sts.SetInt("sid", s.sid); err != nil {
			t.Fatalf("Failed to set sid: %v", err)
		}
		if err := sts.SetString("sname", s.sname); err != nil {
			t.Fatalf("Failed to set sname: %v", err)
		}
		if err := sts.SetInt("gradyear", s.gradyear); err != nil {
			t.Fatalf("Failed to set gradyear: %v", err)
		}
		if err := sts.SetInt("majorid", s.majorid); err != nil {
			t.Fatalf("Failed to set majorid: %v", err)
		}
		t.Logf("Inserting into slot %s: {%d, %s, %d, %d}", sts.GetRid(), s.sid, s.sname, s.gradyear, s.majorid)
	}
}

func setupDepartments(t *testing.T, db *server.SimpleDB, tx *tx.Transaction) {
	dsch := record.NewSchema()
	dsch.AddIntField("did")
	dsch.AddStringField("dname", 3)
	dlayout := record.NewLayout(dsch)
	if err := db.MetadataMgr.CreateTable("department", dsch, tx); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dsts, err := record.NewTableScan(tx, "department", dlayout)
	if err != nil {
		t.Fatalf("Failed to create table scan: %v", err)
	}
	defer dsts.Close()
	for _, d := range departments {
		if err := dsts.Insert(); err != nil {
			t.Fatalf("Failed to insert record: %v", err)
		}
		if err := dsts.SetInt("did", d.did); err != nil {
			t.Fatalf("Failed to set did: %v", err)
		}
		if err := dsts.SetString("dname", d.dname); err != nil {
			t.Fatalf("Failed to set dname: %v", err)
		}
		t.Logf("Inserting into slot %s: {%d, %s}", dsts.GetRid(), d.did, d.dname)
	}
}
