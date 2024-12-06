package metadata

import (
	"fmt"
	"simpledb/internal/record"
	"simpledb/internal/tx"
)

// MaxNameLen is the maximum length of a table name or field name.
const MaxNameLen = 16

// TableMgr manages the metadata for tables in the database.
type TableMgr struct {
	tcatLayout *record.Layout
	fcatLayout *record.Layout
}

// NewTableMgr creates a new table manager, also called a catalog manager.
// If the database is new, the two catalog tables are created.
func NewTableMgr(isNew bool, tx *tx.Transaction) (*TableMgr, error) {
	tm := &TableMgr{}

	tcatSchema := record.NewSchema()
	tcatSchema.AddStringField("tblname", MaxNameLen)
	tcatSchema.AddIntField("slotsize")
	tm.tcatLayout = record.NewLayout(tcatSchema)

	fcatSchema := record.NewSchema()
	fcatSchema.AddStringField("tblname", MaxNameLen)
	fcatSchema.AddStringField("fldname", MaxNameLen)
	fcatSchema.AddIntField("type")
	fcatSchema.AddIntField("length")
	fcatSchema.AddIntField("offset")
	tm.fcatLayout = record.NewLayout(fcatSchema)

	if isNew {
		if err := tm.CreateTable("TableCatalog", tcatSchema, tx); err != nil {
			return nil, err
		}
		if err := tm.CreateTable("FieldCatalog", fcatSchema, tx); err != nil {
			return nil, err
		}
	}
	return tm, nil
}

// CreateTable creates a new table in the database with the specified
// name and schema.
func (tm *TableMgr) CreateTable(tblname string, sch *record.Schema, tx *tx.Transaction) error {
	layout := record.NewLayout(sch)
	// insert one record into tblcat
	tcat, err := record.NewTableScan(tx, "tblcat", tm.tcatLayout)
	if err != nil {
		return err
	}
	if err := tcat.Insert(); err != nil {
		tcat.Close()
		return err
	}
	if err := tcat.SetString("tblname", tblname); err != nil {
		tcat.Close()
		return err
	}
	if err := tcat.SetInt("slotsize", int32(layout.SlotSize)); err != nil {
		tcat.Close()
		return err
	}
	tcat.Close()

	// insert records into fldcat
	fcat, err := record.NewTableScan(tx, "fldcat", tm.fcatLayout)
	if err != nil {
		return err
	}
	for _, name := range sch.Fields {
		if err := fcat.Insert(); err != nil {
			fcat.Close()
			return err
		}
		if err := fcat.SetString("tblname", tblname); err != nil {
			fcat.Close()
			return err
		}
		if err := fcat.SetString("fldname", name); err != nil {
			fcat.Close()
			return err
		}
		if err := fcat.SetInt("type", int32(sch.Type(name))); err != nil {
			fcat.Close()
			return err
		}
		if err := fcat.SetInt("length", int32(sch.Length(name))); err != nil {
			fcat.Close()
			return err
		}
		if err := fcat.SetInt("offset", int32(layout.Offset(name))); err != nil {
			fcat.Close()
			return err
		}
	}
	fcat.Close()
	return nil
}

// GetLayout returns the layout of the specified table.
func (tm *TableMgr) GetLayout(tblname string, tx *tx.Transaction) (*record.Layout, error) {
	tcat, err := record.NewTableScan(tx, "tblcat", tm.tcatLayout)
	if err != nil {
		return nil, err
	}
	defer tcat.Close()

	// Scan tblcat to find the table with the specified name,
	// and get its slot size.
	var slotsize int32 = -1
	for tcat.Next() {
		v, err := tcat.GetString("tblname")
		if err != nil {
			return nil, err
		}
		if v == tblname {
			slotsize, err = tcat.GetInt("slotsize")
			if err != nil {
				return nil, err
			}
			break
		}
	}
	if slotsize == -1 {
		return nil, fmt.Errorf("table %s not found", tblname)
	}
	// TODO: close tcat earlier, instead of deferring

	// Scan fldcat to get the field information for all fields
	// matching the specified table name.
	sch := record.NewSchema()
	offsets := make(map[string]int)
	fcat, err := record.NewTableScan(tx, "fldcat", tm.fcatLayout)
	if err != nil {
		return nil, err
	}
	defer fcat.Close()
	for fcat.Next() {
		v, err := fcat.GetString("tblname")
		if err != nil {
			return nil, err
		}
		if v != tblname {
			continue
		}
		fldname, err := fcat.GetString("fldname")
		if err != nil {
			return nil, err
		}
		fldtype, err := fcat.GetInt("type")
		if err != nil {
			return nil, err
		}
		fldlen, err := fcat.GetInt("length")
		if err != nil {
			return nil, err
		}
		offset, err := fcat.GetInt("offset")
		if err != nil {
			return nil, err
		}
		offsets[fldname] = int(offset)
		sch.AddField(fldname, record.Type(fldtype), int(fldlen))
	}
	return record.NewLayoutFromMetadata(sch, offsets, int(slotsize)), nil
}
