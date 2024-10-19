package file

import "fmt"

// BlockID identifies a single block within a file.
type BlockID struct {
	Filename string
	Blknum   int
}

// NewBlockID creates a new BlockID with the given filename and block number.
func NewBlockID(filename string, blknum int) BlockID {
	return BlockID{filename, blknum}
}

// String returns a string representation of the BlockID.
func (b BlockID) String() string {
	return fmt.Sprintf("[file %s, block %d]", b.Filename, b.Blknum)
}

// Equal checks if two BlockID instances represent the same block.
func (b BlockID) Equal(c BlockID) bool {
	return b.Filename == c.Filename && b.Blknum == c.Blknum
}
