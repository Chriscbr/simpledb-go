package buffer

// BufferAbortError represents an error indicating that a buffer request could
// not be satisfied and the transaction needs to abort.
type BufferAbortError struct{}

// Error implements the error interface for BufferAbortError.
func (e *BufferAbortError) Error() string {
	return "no available buffers"
}

// NewBufferAbortError creates a new BufferAbortError.
func NewBufferAbortError() *BufferAbortError {
	return &BufferAbortError{}
}
