package parse

import "fmt"

// SyntaxError represents a syntax error.
type SyntaxError struct {
	msg string
}

// Error implements the error interface for SyntaxError.
func (e *SyntaxError) Error() string {
	return fmt.Sprintf("syntax error: %s", e.msg)
}

// NewSyntaxError creates a new SyntaxError.
func NewSyntaxError(msg string) *SyntaxError {
	return &SyntaxError{msg}
}
