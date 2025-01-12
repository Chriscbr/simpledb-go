package metadata

import (
	"simpledb/internal/record"
	"simpledb/internal/tx"
	"sync"
)

// StatInfo contains statistics about a table:
// - the number of blocks used by the table
// - the number of records in the table
// - the number of distinct values for each field
type StatInfo struct {
	// The estimated number of blocks in the table.
	BlocksAccessed int
	// The estimated number of records in the table.
	RecordsOutput int
}

// NewStatInfo creates a new StatInfo.
func NewStatInfo(numBlocks int, numRecords int) StatInfo {
	return StatInfo{
		BlocksAccessed: numBlocks,
		RecordsOutput:  numRecords,
	}
}

// DistinctValues returns an estimated number of distinct values for a field.
// This is a complete guess because doing something reasonable is beyond
// the scope of the system.
func (si StatInfo) DistinctValues(fldname string) int {
	return int(1 + si.RecordsOutput/3)
}

// StatMgr manages statistics about each table.
// The manager doesn't store this information in the database;
// instead it calculates this information on startup and
// periodically refreshes it.
type StatMgr struct {
	tm         *TableMgr
	tableStats map[string]*StatInfo
	numCalls   int
	mu         sync.Mutex
}

// NewStatMgr creates a new StatMgr.
// Initial statistics are computed by traversing the entire database.
func NewStatMgr(tm *TableMgr, tx *tx.Transaction) (*StatMgr, error) {
	tablestats := make(map[string]*StatInfo)
	sm := &StatMgr{tm, tablestats, 0, sync.Mutex{}}
	if err := sm.refreshStatistics(tx); err != nil {
		return nil, err
	}
	return sm, nil
}

// GetStatInfo gets the statistics for a specified table.
func (sm *StatMgr) GetStatInfo(tblname string, layout *record.Layout, tx *tx.Transaction) (*StatInfo, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.numCalls++
	if sm.numCalls > 100 {
		if err := sm.refreshStatistics(tx); err != nil {
			return nil, err
		}
	}
	si, ok := sm.tableStats[tblname]
	if !ok {
		var err error
		si, err = sm.calcTableStats(tblname, layout, tx)
		if err != nil {
			return nil, err
		}
		sm.tableStats[tblname] = si
	}
	return si, nil
}

// refreshStatistics refreshes the statistics for all tables.
func (sm *StatMgr) refreshStatistics(tx *tx.Transaction) error {
	sm.tableStats = make(map[string]*StatInfo)
	sm.numCalls = 0
	tcatlayout, err := sm.tm.GetLayout("tblcat", tx)
	if err != nil {
		return err
	}
	tcat, err := record.NewTableScan(tx, "tblcat", tcatlayout)
	if err != nil {
		return err
	}
	defer tcat.Close()
	for tcat.Next() {
		tblname, err := tcat.GetString("tblname")
		if err != nil {
			return err
		}
		layout, err := sm.tm.GetLayout(tblname, tx)
		if err != nil {
			return err
		}
		si, err := sm.calcTableStats(tblname, layout, tx)
		if err != nil {
			return err
		}
		sm.tableStats[tblname] = si
	}
	return nil
}

// calcTableStats calculates the statistics for a specified table.
func (sm *StatMgr) calcTableStats(tblname string, layout *record.Layout, tx *tx.Transaction) (*StatInfo, error) {
	var numBlocks int = 0
	var numRecords int = 0
	ts, err := record.NewTableScan(tx, tblname, layout)
	if err != nil {
		return nil, err
	}
	defer ts.Close()
	for ts.Next() {
		numRecords++
		numBlocks = ts.GetRid().Blknum + 1
	}
	return &StatInfo{numBlocks, numRecords}, nil
}
