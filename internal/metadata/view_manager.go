package metadata

import (
	"simpledb/internal/record"
	"simpledb/internal/tx"
)

// ViewMgr manages views in the database.
type ViewMgr struct {
	tm *TableMgr
}

const MaxViewDefLen = 100

// NewViewMgr creates a new view manager.
func NewViewMgr(isNew bool, tm *TableMgr, tx *tx.Transaction) (*ViewMgr, error) {
	vm := &ViewMgr{tm}
	if isNew {
		sch := record.NewSchema()
		sch.AddStringField("viewname", MaxNameLen)
		sch.AddStringField("viewdef", MaxViewDefLen)
		if err := tm.CreateTable("viewcat", sch, tx); err != nil {
			return nil, err
		}
	}
	return vm, nil
}

// CreateView creates a new view.
func (vm *ViewMgr) CreateView(vname string, vdef string, tx *tx.Transaction) error {
	layout, err := vm.tm.GetLayout("viewcat", tx)
	if err != nil {
		return err
	}
	ts, err := record.NewTableScan(tx, "viewcat", layout)
	if err != nil {
		return err
	}
	if err := ts.Insert(); err != nil {
		return err
	}
	if err := ts.SetString("viewname", vname); err != nil {
		return err
	}
	if err := ts.SetString("viewdef", vdef); err != nil {
		return err
	}
	ts.Close()
	return nil
}

// GetViewDef gets the definition of a specified view.
func (vm *ViewMgr) GetViewDef(vname string, tx *tx.Transaction) (string, error) {
	layout, err := vm.tm.GetLayout("viewcat", tx)
	if err != nil {
		return "", err
	}
	ts, err := record.NewTableScan(tx, "viewcat", layout)
	if err != nil {
		return "", err
	}
	var result string
	for ts.Next() {
		name, err := ts.GetString("viewname")
		if err != nil {
			return "", err
		}
		if name == vname {
			result, err = ts.GetString("viewdef")
			if err != nil {
				return "", err
			}
			break
		}
	}
	ts.Close()
	return result, nil
}
