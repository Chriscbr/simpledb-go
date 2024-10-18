package log

import "simpledb/internal/file"

// LogIterator lets you move through the records of the log file in reverse order.
type LogIterator struct {
	fm         *file.FileMgr
	blk        file.BlockId
	p          *file.Page
	currentpos int
	boundary   int
}

// NewLogIterator creates an iterator for the records in the log file,
// positioned after the last log record.
func NewLogIterator(fm *file.FileMgr, blk file.BlockId) (*LogIterator, error) {
	buf := make([]byte, fm.BlockSize)
	p := file.NewPageFromBytes(buf)
	li := &LogIterator{
		fm:         fm,
		blk:        blk,
		p:          p,
		currentpos: 0,
		boundary:   0,
	}
	if err := li.moveToBlock(blk); err != nil {
		return nil, err
	}
	return li, nil
}

// HasNext returns true if the current log record is the earliest record in the
// log file. (Recall that the log records are written backwards in the file.)
func (li *LogIterator) HasNext() bool {
	return li.currentpos < li.fm.BlockSize || li.blk.Blknum > 0
}

// Next moves to the next log record in the block.
// If there are no more records in the block, then move to the previous block
// and return the log record from there.
func (li *LogIterator) Next() ([]byte, error) {
	if li.currentpos == li.fm.BlockSize {
		li.blk = file.NewBlockId(li.blk.Filename, li.blk.Blknum-1)
		err := li.moveToBlock(li.blk)
		if err != nil {
			return nil, err
		}
	}
	rec := li.p.GetBytes(li.currentpos)
	li.currentpos += 4 + len(rec)
	return rec, nil
}

// moveToBlock moves to the specified log block and positions the iterator at
// the first record in that block (i.e., the most recent one).
func (li *LogIterator) moveToBlock(blk file.BlockId) error {
	err := li.fm.Read(blk, li.p)
	if err != nil {
		return err
	}

	li.boundary = int(li.p.GetInt(0)) // TODO: handle int overflow better
	li.currentpos = li.boundary
	return nil
}
