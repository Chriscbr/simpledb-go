package concurrency

// LockAbortError represents an error indicating that a lock could not
// be obtained and the transaction needs to abort.
type LockAbortError struct{}

// Error implements the error interface for LockAbortError.
func (e *LockAbortError) Error() string {
	return "lock could not be obtained"
}

// NewLockAbortError creates a new LockAbortError.
func NewLockAbortError() *LockAbortError {
	return &LockAbortError{}
}
