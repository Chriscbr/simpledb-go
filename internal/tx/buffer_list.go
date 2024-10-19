package tx

import (
	"simpledb/internal/buffer"
	"simpledb/internal/file"
	"slices"
)

// BufferList manages a transaction's currently-pinned buffers.
type BufferList struct {
	buffers map[file.BlockID]*buffer.Buffer
	// Keep track of how many times each block has been pinned
	pins []file.BlockID
	bm   *buffer.BufferMgr
}

// NewBufferList creates a new BufferList instance
func NewBufferList(bm *buffer.BufferMgr) *BufferList {
	return &BufferList{
		buffers: make(map[file.BlockID]*buffer.Buffer),
		pins:    make([]file.BlockID, 0),
		bm:      bm,
	}
}

// GetBuffer returns the buffer pinned to the specified block.
// The method returns nil if the transaction has not pinned the block.
func (bl *BufferList) GetBuffer(blk file.BlockID) *buffer.Buffer {
	return bl.buffers[blk]
}

// Pin pins the specified block and keeps track of the buffer internally.
func (bl *BufferList) Pin(blk file.BlockID) error {
	b, err := bl.bm.Pin(blk)
	if err != nil {
		return err
	}

	bl.buffers[blk] = b
	bl.pins = append(bl.pins, blk)

	return nil
}

// Unpin unpins the specified block.
func (bl *BufferList) Unpin(blk file.BlockID) {
	b, exists := bl.buffers[blk]
	if !exists {
		return
	}
	bl.bm.Unpin(b)
	bl.removePin(blk)
	if !slices.Contains(bl.pins, blk) {
		delete(bl.buffers, blk)
	}
}

// UnpinAll unpins any buffers still pinned by this transaction.
func (bl *BufferList) UnpinAll() {
	for _, blk := range bl.pins {
		b := bl.buffers[blk]
		bl.bm.Unpin(b)
	}
	bl.buffers = make(map[file.BlockID]*buffer.Buffer)
	bl.pins = make([]file.BlockID, 0)
}

// removePin removes a block from the pins slice.
func (bl *BufferList) removePin(blk file.BlockID) {
	for i, pin := range bl.pins {
		if pin == blk {
			bl.pins = append(bl.pins[:i], bl.pins[i+1:]...)
			return
		}
	}
}
