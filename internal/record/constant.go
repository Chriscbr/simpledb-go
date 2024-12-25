package record

import (
	"fmt"
	"hash/fnv"
)

// Constant represents a value in the database.
type Constant struct {
	ival *int32  // using pointer to represent nullable integer
	sval *string // using pointer to represent nullable string
}

// NewIntConstant creates a new Constant with an integer value
func NewIntConstant(val int32) Constant {
	return Constant{ival: &val}
}

// NewStringConstant creates a new Constant with a string value
func NewStringConstant(val string) Constant {
	return Constant{sval: &val}
}

// AsInt returns the integer value
func (c Constant) AsInt() int32 {
	if c.ival == nil {
		panic("Constant does not contain an integer value")
	}
	return *c.ival
}

// AsString returns the string value
func (c Constant) AsString() string {
	if c.sval == nil {
		panic("Constant does not contain a string value")
	}
	return *c.sval
}

// Equal implements value comparison for Constant
func (c Constant) Equal(other Constant) bool {
	if c.ival != nil && other.ival != nil {
		return *c.ival == *other.ival
	}
	if c.sval != nil && other.sval != nil {
		return *c.sval == *other.sval
	}
	return false
}

// Compare implements comparison for Constant
// Returns -1 if c < other, 0 if c == other, and 1 if c > other
func (c Constant) Compare(other Constant) int {
	if c.ival != nil && other.ival != nil {
		switch {
		case *c.ival < *other.ival:
			return -1
		case *c.ival > *other.ival:
			return 1
		default:
			return 0
		}
	}
	if c.sval != nil && other.sval != nil {
		switch {
		case *c.sval < *other.sval:
			return -1
		case *c.sval > *other.sval:
			return 1
		default:
			return 0
		}
	}
	panic("Cannot compare constants of different types")
}

// Hash returns the hash value of the constant
func (c Constant) Hash() int {
	if c.ival != nil {
		return int(*c.ival)
	}
	if c.sval != nil {
		h := fnv.New32a()
		h.Write([]byte(*c.sval))
		return int(h.Sum32())
	}
	panic("Constant does not contain a value")
}

// String implements the Stringer interface
func (c Constant) String() string {
	if c.ival != nil {
		return fmt.Sprintf("%d", *c.ival)
	}
	if c.sval != nil {
		return fmt.Sprintf("'%s'", *c.sval)
	}
	panic("Constant does not contain a value")
}
