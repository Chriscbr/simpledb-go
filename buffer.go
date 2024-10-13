package simpledb

// Manages the pinning and unpinning of buffers to blocks.
type BufferMgr struct{}

// Creates a new BufferMgr instance with the given number of buffer slots.
func NewBufferMgr(fm *FileMgr, lm *LogMgr, numbufs int) (*BufferMgr, error) {
	bm := &BufferMgr{}
	return bm, nil
}
