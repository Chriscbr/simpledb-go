package file

import "encoding/binary"

// Page represents a fixed-size block of data in memory.
type Page struct {
	buf []byte
}

// NewPage creates a new Page with the given blocksize.
func NewPage(blocksize int) *Page {
	buf := make([]byte, blocksize)
	return &Page{buf}
}

// NewPageFromBytes creates a new Page from the given byte slice.
func NewPageFromBytes(buf []byte) *Page {
	return &Page{buf}
}

// GetInt retrieves an int at the specified offset.
func (p *Page) GetInt(offset int) int32 {
	return int32(binary.BigEndian.Uint32(p.buf[offset : offset+4]))
}

// SetInt sets an int at the specified offset.
func (p *Page) SetInt(offset int, n int32) {
	binary.BigEndian.PutUint32(p.buf[offset:offset+4], uint32(n))
}

// GetBytes retrieves a byte slice at the specified offset.
func (p *Page) GetBytes(offset int) []byte {
	length := int(p.GetInt(offset))
	return p.buf[offset+4 : offset+4+length]
}

// SetBytes sets a byte slice at the specified offset.
func (p *Page) SetBytes(offset int, b []byte) {
	length := len(b)
	p.SetInt(offset, int32(length))
	copy(p.buf[offset+4:offset+4+length], b)
}

// GetString retrieves a string at the specified offset.
func (p *Page) GetString(offset int) string {
	return string(p.GetBytes(offset))
}

// SetString sets a string at the specified offset.
func (p *Page) SetString(offset int, s string) {
	p.SetBytes(offset, []byte(s))
}

// MaxLength calculates the number of bytes needed to store a string with the
// given length.
func MaxLength(strlen int) int {
	return 4 + strlen
}
