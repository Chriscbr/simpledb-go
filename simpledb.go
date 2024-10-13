package simpledb

type SimpleDB struct {
	fm *FileMgr
	lm *LogMgr
	bm *BufferMgr
}

const LogFile = "simpledb.log"

// Creates a new SimpleDB instance with the given directory name and blocksize.
func NewSimpleDB(dirname string, blocksize int, numbufs int) (*SimpleDB, error) {
	fm, err := NewFileMgr(dirname, blocksize)
	if err != nil {
		return nil, err
	}

	lm, err := NewLogMgr(fm, LogFile)
	if err != nil {
		return nil, err
	}

	bm, err := NewBufferMgr(fm, lm, numbufs)
	if err != nil {
		return nil, err
	}

	db := &SimpleDB{fm, lm, bm}
	return db, nil
}

// Closes the SimpleDB instance
func (db *SimpleDB) Close() {
	db.fm.Close()
}
