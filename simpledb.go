package simpledb

type SimpleDB struct {
	fm *FileMgr
	lm *LogMgr
}

const LogFile = "simpledb.log"

// Creates a new SimpleDB instance with the given directory name and blocksize.
func NewSimpleDB(dirname string, blocksize int) (*SimpleDB, error) {
	fm, err := NewFileMgr(dirname, blocksize)
	if err != nil {
		return nil, err
	}

	lm, err := NewLogMgr(fm, LogFile)
	if err != nil {
		return nil, err
	}

	db := &SimpleDB{fm, lm}
	return db, nil
}

// Closes the SimpleDB instance
func (db *SimpleDB) Close() {
	db.fm.Close()
}
