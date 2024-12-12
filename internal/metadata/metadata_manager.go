package metadata

import (
	"simpledb/internal/record"
	"simpledb/internal/tx"
)

var tblMgr *TableMgr
var viewMgr *ViewMgr
var idxMgr *IndexMgr
var statMgr *StatMgr

type MetadataMgr struct{}

func NewMetadataMgr(isNew bool, tx *tx.Transaction) (*MetadataMgr, error) {
	var err error
	tblMgr, err = NewTableMgr(isNew, tx)
	if err != nil {
		return nil, err
	}
	viewMgr, err = NewViewMgr(isNew, tblMgr, tx)
	if err != nil {
		return nil, err
	}
	statMgr, err = NewStatMgr(tblMgr, tx)
	if err != nil {
		return nil, err
	}
	idxMgr, err = NewIndexMgr(isNew, tblMgr, statMgr, tx)
	if err != nil {
		return nil, err
	}
	return &MetadataMgr{}, nil
}

func (mm *MetadataMgr) CreateTable(tblname string, sch *record.Schema, tx *tx.Transaction) error {
	return tblMgr.CreateTable(tblname, sch, tx)
}

func (mm *MetadataMgr) GetLayout(tblname string, tx *tx.Transaction) (*record.Layout, error) {
	return tblMgr.GetLayout(tblname, tx)
}

func (mm *MetadataMgr) CreateView(viewname string, viewdef string, tx *tx.Transaction) error {
	return viewMgr.CreateView(viewname, viewdef, tx)
}

func (mm *MetadataMgr) GetViewDef(viewname string, tx *tx.Transaction) (string, error) {
	return viewMgr.GetViewDef(viewname, tx)
}

func (mm *MetadataMgr) CreateIndex(idxname, tblname, fldname string, tx *tx.Transaction) error {
	return idxMgr.CreateIndex(idxname, tblname, fldname, tx)
}

func (mm *MetadataMgr) GetIndexInfo(tblname string, tx *tx.Transaction) (map[string]*IndexInfo, error) {
	return idxMgr.GetIndexInfo(tblname, tx)
}

func (mm *MetadataMgr) GetStatInfo(tblname string, tblLayout *record.Layout, tx *tx.Transaction) (*StatInfo, error) {
	return statMgr.GetStatInfo(tblname, tblLayout, tx)
}
