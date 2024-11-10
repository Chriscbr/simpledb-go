package record

// Layout describes the structure of a record.
// It contains the name, type, length, and offset of each field of the table.
type Layout struct {
	Schema   *Schema
	offsets  map[string]int
	SlotSize int
}

// NewLayout creates a Layout object from a schema.
// It's used when a table is created, and it determines the physical offset
// of each field within the record.
func NewLayout(schema *Schema) (*Layout, error) {
	offsets := make(map[string]int)
	pos := 4 // leave space for the empty/inuse flag
	for _, name := range schema.Fields {
		offsets[name] = pos
		length, err := schema.LengthInBytes(name)
		if err != nil {
			return nil, err
		}
		pos += length
	}
	layout := &Layout{
		Schema:   schema,
		offsets:  offsets,
		SlotSize: pos,
	}
	return layout, nil
}

// NewLayoutFromMetadata creates a Layout object from the specified metadata.
// This constructor is used when the metadata is retrieved from the catalog.
func NewLayoutFromMetadata(schema *Schema, offsets map[string]int, slotsize int) *Layout {
	return &Layout{schema, offsets, slotsize}
}

// Offset returns the offset of a specified field within a record.
// Returns ErrFieldNotFound if the field doesn't exist.
func (l *Layout) Offset(name string) (int, error) {
	offset, ok := l.offsets[name]
	if !ok {
		return 0, ErrFieldNotFound
	}
	return offset, nil
}
