package simpledb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type BlockId struct {
	filename string
	blknum   int
}

// Creates a new BlockId with the given filename and block number.
func NewBlockId(filename string, blknum int) *BlockId {
	bid := &BlockId{filename, blknum}
	return bid
}

// Returns a string representation of the BlockId.
func (b *BlockId) String() string {
	return fmt.Sprintf("[file %s, block %d]", b.filename, b.blknum)
}

type Page struct {
	buf []byte
}

// Creates a new Page with the given blocksize.
func NewPage(blocksize int) *Page {
	buf := make([]byte, blocksize)
	p := &Page{buf}
	return p
}

// Creates a new Page from the given byte slice.
func NewPageFromBytes(buf []byte) *Page {
	p := &Page{buf}
	return p
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

type FileMgr struct {
	dbdir     string
	BlockSize int
	IsNew     bool
	openFiles map[string]*os.File
	mu        sync.Mutex
}

// Creates a new FileMgr with the given directory name and blocksize.
func NewFileMgr(dbdir string, blocksize int) (*FileMgr, error) {
	fm := &FileMgr{
		dbdir:     dbdir,
		BlockSize: blocksize,
		openFiles: make(map[string]*os.File),
	}

	// Check if the directory exists
	info, err := os.Stat(dbdir)
	if os.IsNotExist(err) {
		fm.IsNew = true
		if err := os.MkdirAll(dbdir, 0755); err != nil {
			return nil, fmt.Errorf("cannot create directory %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("cannot access directory %w", err)
	} else if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", dbdir)
	}

	// Remove temporary files
	files, err := os.ReadDir(dbdir)
	if err != nil {
		return nil, fmt.Errorf("cannot read directory %w", err)
	}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "temp") {
			if err := os.Remove(filepath.Join(dbdir, file.Name())); err != nil {
				return nil, fmt.Errorf("cannot remove file %s: %w", file.Name(), err)
			}
		}
	}

	return fm, nil
}

// Closes all open files
func (fm *FileMgr) Close() {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	for _, f := range fm.openFiles {
		f.Close()
	}
}

// Reads the contents of the specified block into the specified page
func (fm *FileMgr) Read(blk *BlockId, p *Page) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	f, err := fm.getFile(blk.filename)
	if err != nil {
		return err
	}

	offset := int64(blk.blknum) * int64(fm.BlockSize)
	if _, err := f.ReadAt(p.buf, offset); err != nil {
		return fmt.Errorf("cannot read block %s: %w", blk, err)
	}

	return nil
}

// Writes the contents of a page to the specified block
func (fm *FileMgr) Write(blk *BlockId, p *Page) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	f, err := fm.getFile(blk.filename)
	if err != nil {
		return err
	}

	offset := int64(blk.blknum) * int64(fm.BlockSize)
	if _, err := f.WriteAt(p.buf, offset); err != nil {
		return fmt.Errorf("cannot write block %s: %w", blk, err)
	}

	return nil
}

// Seeks to the end of the file and writes an empty array of bytes, extending the file
func (fm *FileMgr) Append(filename string) (*BlockId, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	f, err := fm.getFile(filename)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("cannot stat file %s: %w", filename, err)
	}

	newblknum := int(info.Size() / int64(fm.BlockSize))
	buf := make([]byte, fm.BlockSize) // an empty block of data
	offset := int64(newblknum * fm.BlockSize)
	if _, err := f.WriteAt(buf, offset); err != nil {
		return nil, fmt.Errorf("cannot write to file %s: %w", filename, err)
	}

	return NewBlockId(filename, newblknum), nil
}

// Returns the number of blocks in the specified file
func (fm *FileMgr) Length(filename string) (int, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	f, err := fm.getFile(filename)
	if err != nil {
		return 0, err
	}

	info, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("cannot stat file %s: %w", filename, err)
	}

	return int(info.Size() / int64(fm.BlockSize)), nil
}

func (fm *FileMgr) getFile(filename string) (*os.File, error) {
	if f, ok := fm.openFiles[filename]; ok {
		return f, nil
	}

	path := filepath.Join(fm.dbdir, filename)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_SYNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("cannot open file %s: %w", path, err)
	}

	fm.openFiles[filename] = f
	return f, nil
}
