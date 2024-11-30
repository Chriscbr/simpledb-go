package record

import "fmt"

// RID is an identifier for a record within a file.
// It consists of a block number and the slot number within that block.
type RID struct {
	Blknum int
	Slot   int
}

// NewRID creates a new RID with the given block number and slot number.
func NewRID(blk int, slot int) RID {
	return RID{Blknum: blk, Slot: slot}
}

// String returns a string representation of the RID.
func (rid RID) String() string {
	return fmt.Sprintf("(%d, %d)", rid.Blknum, rid.Slot)
}

// Equal checks if two RID instances represent the same record.
func (rid RID) Equal(other RID) bool {
	return rid.Blknum == other.Blknum && rid.Slot == other.Slot
}
