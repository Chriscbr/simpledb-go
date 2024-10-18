package tx

import (
	"simpledb/internal/buffer"
	"simpledb/internal/file"
)

// BufferList manages a transaction's currently-pinned buffers.
type BufferList struct {
	buffers map[file.BlockId]*buffer.Buffer
	// Keep track of how many times each block has been pinned
	pins []file.BlockId
	bm   *buffer.BufferMgr
}

// Creates a new BufferList instance
func NewBufferList(bm *buffer.BufferMgr) *BufferList {
	return &BufferList{
		buffers: make(map[file.BlockId]*buffer.Buffer),
		pins:    make([]file.BlockId, 0),
		bm:      bm,
	}
}

// Return the buffer pinned to the specified block.
// The method returns null if the transaction has not pinned the block.
func (bl *BufferList) GetBuffer(blk file.BlockId) *buffer.Buffer {
	return bl.buffers[blk]
}

// Pin the block and keep track of the buffer internally.
func (bl *BufferList) Pin(blk file.BlockId) error {
	b, err := bl.bm.Pin(blk)
	if err != nil {
		return err
	}

	bl.buffers[blk] = b
	bl.pins = append(bl.pins, blk)

	return nil
}

// Unpin the specified block.
func (bl *BufferList) Unpin(blk file.BlockId) {
	b, exists := bl.buffers[blk]
	if !exists {
		return
	}
	bl.bm.Unpin(b)
	bl.removePin(blk)
	if !bl.containsPin(blk) {
		delete(bl.buffers, blk)
	}
}

// Unpins any buffers still pinned by this transaction.
func (bl *BufferList) UnpinAll() {
	for _, blk := range bl.pins {
		b := bl.buffers[blk]
		bl.bm.Unpin(b)
	}
	bl.buffers = make(map[file.BlockId]*buffer.Buffer)
	bl.pins = make([]file.BlockId, 0)
}

// Removes a block from the pins slice.
func (bl *BufferList) removePin(blk file.BlockId) {
	for i, pin := range bl.pins {
		if pin == blk {
			bl.pins = append(bl.pins[:i], bl.pins[i+1:]...)
			return
		}
	}
}

// Checks if a block is in the pins slice.
func (bl *BufferList) containsPin(blk file.BlockId) bool {
	for _, pin := range bl.pins {
		if pin == blk {
			return true
		}
	}
	return false
}
