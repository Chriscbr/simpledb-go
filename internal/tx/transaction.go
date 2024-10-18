package tx

import (
	"simpledb/internal/file"
)

type Transaction struct{}

func NewTransaction() *Transaction {
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

func (t *Transaction) Pin(blk file.BlockId) {
	// TODO: implement
}

func (t *Transaction) Unpin(blk file.BlockId) {
	// TODO: implement
}

func (t *Transaction) GetInt(blk file.BlockId, offset int) int {
	// TODO: implement
	return 0
}

func (t *Transaction) GetString(blk file.BlockId, offset int) string {
	// TODO: implement
	return ""
}

func (t *Transaction) SetInt(blk file.BlockId, offset int, value int, okToLog bool) {
	// TODO: implement
}

func (t *Transaction) SetString(blk file.BlockId, offset int, value string, okToLog bool) {
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

func (t *Transaction) Append(filename string) file.BlockId {
	// TODO: implement
	return file.BlockId{}
}

func (t *Transaction) BlockSize() int {
	// TODO: implement
	return 0
}
