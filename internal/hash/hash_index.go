package hash

import (
	"fmt"
	"simpledb/internal/index"
	"simpledb/internal/record"
	"simpledb/internal/tx"
)

// HashIndex provides a static hash implementation of the Index interface.
// A fixed number of buckets is allocated (currently, 100),
// and each bucket is implemented as a file of index records.
type HashIndex struct {
	tx        *tx.Transaction
	idxName   string
	layout    *record.Layout
	searchKey *record.Constant  // initially nil
	ts        *record.TableScan // initially nil
}

// Check that HashIndex implements Index
var _ index.Index = (*HashIndex)(nil)

const NumBuckets = 100

// NewHashIndex creates a new HashIndex object.
func NewHashIndex(tx *tx.Transaction, idxName string, layout *record.Layout) *HashIndex {
	return &HashIndex{tx: tx, idxName: idxName, layout: layout}
}

// BeforeFirst positions the index before the first record
// having the specified search key.
// The method hashes the search key to determine the bucket, and then
// opens a table scan on the file corresponding to the bucket.
// The table scan for the previous bucket (if any) is closed.
func (hi *HashIndex) BeforeFirst(searchkey record.Constant) error {
	hi.Close()
	hi.searchKey = &searchkey
	bucket := searchkey.Hash() % NumBuckets
	tblname := fmt.Sprintf("%s%d", hi.idxName, bucket)
	ts, err := record.NewTableScan(hi.tx, tblname, hi.layout)
	if err != nil {
		return err
	}
	hi.ts = ts
	return nil
}

// Next moves the index to the next record having the search key.
// The method loops through the table scan for the bucket,
// looking for a matching record, and returning false if there are
// no more such records.
func (hi *HashIndex) Next() bool {
	if hi.ts == nil {
		return false
	}
	for hi.ts.Next() {
		dataval, err := hi.ts.GetVal("dataval")
		if err != nil {
			panic(err)
		}
		if dataval.Equal(*hi.searchKey) {
			return true
		}
	}
	return false
}

// GetDataRID returns the RID of the current record
// in the table scan for the bucket.
func (hi *HashIndex) GetDataRID() (record.RID, error) {
	blknum, err := hi.ts.GetInt("block")
	if err != nil {
		return record.RID{}, err
	}
	slot, err := hi.ts.GetInt("id")
	if err != nil {
		return record.RID{}, err
	}
	return record.RID{Blknum: int(blknum), Slot: int(slot)}, nil
}

// Insert adds a new record into the table scan for the bucket.
func (hi *HashIndex) Insert(val record.Constant, rid record.RID) error {
	err := hi.BeforeFirst(val)
	if err != nil {
		return err
	}
	err = hi.ts.Insert()
	if err != nil {
		return err
	}
	err = hi.ts.SetInt("block", int32(rid.Blknum))
	if err != nil {
		return err
	}
	err = hi.ts.SetInt("id", int32(rid.Slot))
	if err != nil {
		return err
	}
	err = hi.ts.SetVal("dataval", val)
	if err != nil {
		return err
	}
	return nil
}

// Delete removes a record from the table scan for the bucket.
// The method starts at the beginning of the scan, and loops through the
// records until the specified record is found.
func (hi *HashIndex) Delete(val record.Constant, rid record.RID) error {
	err := hi.BeforeFirst(val)
	if err != nil {
		return err
	}
	for hi.Next() {
		currRID, err := hi.GetDataRID()
		if err != nil {
			return err
		}
		if currRID.Equal(rid) {
			return hi.ts.Delete()
		}
	}
	return nil
}

// Close closes the index.
func (hi *HashIndex) Close() {
	if hi.ts != nil {
		hi.ts.Close()
		hi.ts = nil
	}
}

// SearchCost returns the cost of searching an index file having the
// specified number of blocks.
// The method assumes that all buckets are about the same size, and so the
// cost is simply the size of the bucket.
func SearchCost(numblocks int, recordPerBlock int) int {
	return numblocks / NumBuckets
}
