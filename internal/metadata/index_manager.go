package metadata

import (
	"simpledb/internal/hash"
	"simpledb/internal/record"
	"simpledb/internal/tx"
)

// IndexInfo contains information about an index.
// This information is used by the query planner in order to estimate the
// costs of using the index, and to obtain the layout of the index records.
// Its methods are essentially the same as those of Plan.
type IndexInfo struct {
	idxName   string
	fldName   string
	tx        *tx.Transaction
	tblSchema *record.Schema
	idxLayout *record.Layout
	tblStats  *StatInfo
}

// NewIndexInfo creates an IndexInfo object for the specified index.
func NewIndexInfo(idxname, fldname string, tblSchema *record.Schema, tx *tx.Transaction, si *StatInfo) (*IndexInfo, error) {
	ii := &IndexInfo{idxname, fldname, tx, tblSchema, nil, si}
	ii.idxLayout = ii.createIdxLayout()
	return ii, nil
}

// Open opens the index described by this object.
func (ii *IndexInfo) Open() *hash.HashIndex {
	return hash.NewHashIndex(ii.tx, ii.idxName, ii.idxLayout)
}

// BlocksAccessed estimates the number of block accesses required to find all
// index records having a particular search key.
// The method uses the table's metadata to estimate the size of the index file
// and the number of index records per block.
// It then passes this information to the traversalCost method of the appropriate
// index type, which provides the estimate.
func (ii *IndexInfo) BlocksAccessed() int {
	recordsPerBlock := ii.tx.BlockSize() / ii.idxLayout.SlotSize
	numBlocks := ii.tblStats.RecordsOutput / recordsPerBlock
	return hash.SearchCost(numBlocks, recordsPerBlock)
}

// RecordsOutput estimates the number of records having a search key.
// This value is the same as doing a select query; that is, it is the number
// of records in the table divided by the number of distinct values of the indexed field.
func (ii *IndexInfo) RecordsOutput() int {
	return ii.tblStats.RecordsOutput / ii.tblStats.DistinctValues(ii.fldName)
}

// DistinctValues returns the number of distinct values for the indexed field,
// or 1 for the indexed field.
func (ii *IndexInfo) DistinctValues(fname string) int {
	if fname == ii.fldName {
		return 1
	}
	return ii.tblStats.DistinctValues(fname)
}

// createIdxLayout returns the layout of the index records.
// The schema consists of the dataRID (which is represented as two integers,
// the block number and the record ID) and the dataval (which is the indexed
// field). Schema information about the indexed field is obtained via the
// table's schema.
func (ii *IndexInfo) createIdxLayout() *record.Layout {
	sch := record.NewSchema()
	sch.AddIntField("block")
	sch.AddIntField("id")
	if ii.tblSchema.Type(ii.fldName) == record.Integer {
		sch.AddIntField("dataval")
	} else {
		fldlen := ii.tblSchema.Length(ii.fldName)
		sch.AddStringField("dataval", fldlen)
	}
	return record.NewLayout(sch)
}

// IndexMgr manages indexes in the database.
type IndexMgr struct {
	layout *record.Layout
	tm     *TableMgr
	sm     *StatMgr
}

// NewIndexMgr creates a new index manager.
func NewIndexMgr(isNew bool, tm *TableMgr, sm *StatMgr, tx *tx.Transaction) (*IndexMgr, error) {
	im := &IndexMgr{tm: tm, sm: sm}
	if isNew {
		sch := record.NewSchema()
		sch.AddStringField("indexname", MaxNameLen)
		sch.AddStringField("tablename", MaxNameLen)
		sch.AddStringField("fieldname", MaxNameLen)
		if err := im.tm.CreateTable("idxcat", sch, tx); err != nil {
			return nil, err
		}
	}
	return im, nil
}

// CreateIndex creates an index of the specified type for the specified field.
// A unique ID is assigned to this index, and its information is stored in the
// idxcat table.
func (im *IndexMgr) CreateIndex(idxname, tblname, fldname string, tx *tx.Transaction) error {
	ts, err := record.NewTableScan(tx, "idxcat", im.layout)
	if err != nil {
		return err
	}
	defer ts.Close()
	if err := ts.Insert(); err != nil {
		return err
	}
	if err := ts.SetString("indexname", idxname); err != nil {
		return err
	}
	if err := ts.SetString("tablename", tblname); err != nil {
		return err
	}
	if err := ts.SetString("fieldname", fldname); err != nil {
		return err
	}
	return nil
}

// GetIndexInfo returns a map containing the index info for all indices.
func (im *IndexMgr) GetIndexInfo(tblname string, tx *tx.Transaction) (map[string]*IndexInfo, error) {
	var result = make(map[string]*IndexInfo)
	ts, err := record.NewTableScan(tx, "idxcat", im.layout)
	if err != nil {
		return nil, err
	}
	defer ts.Close()
	for ts.Next() {
		currTblName, err := ts.GetString("tablename")
		if err != nil {
			return nil, err
		}
		if currTblName == tblname {
			idxname, err := ts.GetString("indexname")
			if err != nil {
				return nil, err
			}
			fldname, err := ts.GetString("fieldname")
			if err != nil {
				return nil, err
			}
			tblLayout, err := im.tm.GetLayout(tblname, tx)
			if err != nil {
				return nil, err
			}
			tblStats, err := im.sm.GetStatInfo(tblname, tblLayout, tx)
			if err != nil {
				return nil, err
			}
			ii, err := NewIndexInfo(idxname, fldname, tblLayout.Schema, tx, tblStats)
			if err != nil {
				return nil, err
			}
			result[fldname] = ii
		}
	}
	return result, nil
}
