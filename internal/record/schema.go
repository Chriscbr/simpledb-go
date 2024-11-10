package record

import (
	"errors"
	"simpledb/internal/file"
)

type Type int

const (
	Integer Type = iota
	String
)

// String implements the Stringer interface for Type
func (t Type) String() string {
	switch t {
	case Integer:
		return "Integer"
	case String:
		return "String"
	default:
		return "Unknown"
	}
}

// Schema represents the record schema of a table.
// It contains the name and type of each field of the table,
// as well as the length of each varchar field.
type Schema struct {
	Fields []string
	info   map[string]FieldInfo
}

// FieldInfo contains the type and length information for a field.
type FieldInfo struct {
	typ    Type
	length int
}

var ErrFieldNotFound = errors.New("schema does not have field with specified name")
var ErrFieldUnknownType = errors.New("schema field has an unknown type")

// NewSchema creates a new Schema instance.
func NewSchema() *Schema {
	return &Schema{
		Fields: make([]string, 0),
		info:   make(map[string]FieldInfo),
	}
}

// AddField adds a field to the schema with a specified
// name, type, and length.
// If the field type is integer, then the length value is ignored.
func (s *Schema) AddField(name string, typ Type, length int) {
	s.Fields = append(s.Fields, name)
	s.info[name] = FieldInfo{typ, length}
}

// AddIntField adds an integer field to the schema.
func (s *Schema) AddIntField(name string) {
	s.AddField(name, Integer, 0)
}

// AddStringField adds a string field to the schema.
// The length is the conceptual length of the field.
// For example, if the field is defined as varchar(8),
// then its length is 8.
func (s *Schema) AddStringField(name string, length int) {
	s.AddField(name, String, length)
}

// Add adds a field to the schema having the same type and length
// as the corresponding field in another schema.
// Returns an error if the name does not exist in the other schema.
func (s *Schema) Add(name string, sch *Schema) error {
	info, ok := sch.info[name]
	if !ok {
		return ErrFieldNotFound
	}
	s.AddField(name, info.typ, info.length)
	return nil
}

// AddAll adds all of the fields in the specified schema
// to the current schema.
func (s *Schema) AddAll(sch *Schema) {
	for _, name := range sch.Fields {
		s.AddField(name, sch.info[name].typ, sch.info[name].length)
	}
}

// HasField returns true if the schema has a field with the specified name.
func (s *Schema) HasField(name string) bool {
	_, ok := s.info[name]
	return ok
}

// Type returns the type of the specified field.
// Returns ErrFieldNotFound if the field doesn't exist.
func (s *Schema) Type(name string) (Type, error) {
	info, ok := s.info[name]
	if !ok {
		return Type(0), ErrFieldNotFound
	}
	return info.typ, nil
}

// Length returns the conceptual length of the specified field.
// Returns ErrFieldNotFound if the field doesn't exist.
// If the field is not a string field, the length is undefined.
func (s *Schema) Length(name string) (int, error) {
	info, ok := s.info[name]
	if !ok {
		return 0, ErrFieldNotFound
	}
	return info.length, nil
}

// LengthInBytes returns the number of bytes needed to represent
// the specified field.
// Returns ErrFieldNotfound if the field doesn't exist.
func (s *Schema) LengthInBytes(name string) (int, error) {
	info, ok := s.info[name]
	if !ok {
		return 0, ErrFieldNotFound
	}
	switch info.typ {
	case Integer:
		return 4, nil
	case String:
		return file.MaxLength(info.length), nil
	default:
		return 0, ErrFieldUnknownType
	}
}
