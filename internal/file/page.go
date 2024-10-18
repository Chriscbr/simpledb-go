package file

import "encoding/binary"

// Page represents a fixed-size block of data in memory.
type Page struct {
	buf []byte
}

// Creates a new Page with the given blocksize.
func NewPage(blocksize int) *Page {
	buf := make([]byte, blocksize)
	return &Page{buf}
}

// Creates a new Page from the given byte slice.
func NewPageFromBytes(buf []byte) *Page {
	return &Page{buf}
}

// Retrieves int at the specified offset.
func (p *Page) GetInt(offset int) int32 {
	return int32(binary.BigEndian.Uint32(p.buf[offset : offset+4]))
}

// Sets an int at the specified offset.
func (p *Page) SetInt(offset int, n int32) {
	binary.BigEndian.PutUint32(p.buf[offset:offset+4], uint32(n))
}

// Retrieves a byte slice at the specified offset.
func (p *Page) GetBytes(offset int) []byte {
	length := int(p.GetInt(offset))
	return p.buf[offset+4 : offset+4+length]
}

// Sets a byte slice at the specified offset.
func (p *Page) SetBytes(offset int, b []byte) {
	length := len(b)
	p.SetInt(offset, int32(length))
	copy(p.buf[offset+4:offset+4+length], b)
}

// Retrieves a string at the specified offset.
func (p *Page) GetString(offset int) string {
	return string(p.GetBytes(offset))
}

// Sets a string at the specified offset.
func (p *Page) SetString(offset int, s string) {
	p.SetBytes(offset, []byte(s))
}

// Calculates the number of bytes needed to store a string with the given length.
func MaxLength(strlen int) int {
	return 4 + strlen
}
