package file

import "fmt"

// BlockId identifies a single block within a file.
type BlockId struct {
	Filename string
	Blknum   int
}

// Creates a new BlockId with the given filename and block number.
func NewBlockId(filename string, blknum int) *BlockId {
	bid := &BlockId{filename, blknum}
	return bid
}

// Returns a string representation of the BlockId.
func (b *BlockId) String() string {
	return fmt.Sprintf("[file %s, block %d]", b.Filename, b.Blknum)
}

// Equals checks if two BlockId instances represent the same block.
func (b *BlockId) Equal(c *BlockId) bool {
	if c == nil {
		return false
	}
	return b.Filename == c.Filename && b.Blknum == c.Blknum
}
