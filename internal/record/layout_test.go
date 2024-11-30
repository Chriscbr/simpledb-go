package record_test

import (
	"simpledb/internal/record"
	"testing"
)

func TestLayout(t *testing.T) {
	sch := record.NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	layout := record.NewLayout(sch)

	expectedOffsets := map[string]int{
		"A": 4,
		"B": 8,
	}

	for _, fieldName := range layout.Schema.Fields {
		offset := layout.Offset(fieldName)
		expected := expectedOffsets[fieldName]
		if offset != expected {
			t.Errorf("Field %s: expected offset %d, got offset %d",
				fieldName, expected, offset)
		}
	}
}
