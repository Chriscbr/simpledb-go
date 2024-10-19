package tx

import (
	"simpledb/internal/buffer"
	"simpledb/internal/file"
	"simpledb/internal/log"
)

// Transaction provides transaction management for clients, ensuring that
// all transactions are serializable, recoverable, and in general satisfy
// the ACID properties.
type Transaction struct{}

func NewTransaction(fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr) *Transaction {
	return &Transaction{}
}

func (t *Transaction) Commit() {
	// TODO: implement
}

func (t *Transaction) Rollback() {
	// TODO: implement
}

func (t *Transaction) Recover() {
	// TODO: implement
}

func (t *Transaction) Pin(blk file.BlockID) {
	// TODO: implement
}

func (t *Transaction) Unpin(blk file.BlockID) {
	// TODO: implement
}

func (t *Transaction) GetInt(blk file.BlockID, offset int) int {
	// TODO: implement
	return 0
}

func (t *Transaction) GetString(blk file.BlockID, offset int) string {
	// TODO: implement
	return ""
}

func (t *Transaction) SetInt(blk file.BlockID, offset int, value int, okToLog bool) {
	// TODO: implement
}

func (t *Transaction) SetString(blk file.BlockID, offset int, value string, okToLog bool) {
	// TODO: implement
}

func (t *Transaction) AvailableBufs() int {
	// TODO: implement
	return 0
}

func (t *Transaction) Size(filename string) int {
	// TODO: implement
	return 0
}

func (t *Transaction) Append(filename string) file.BlockID {
	// TODO: implement
	return file.BlockID{}
}

func (t *Transaction) BlockSize() int {
	// TODO: implement
	return 0
}
