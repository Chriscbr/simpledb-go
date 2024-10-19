package file

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// FileMgr manages raw file access for the database.
type FileMgr struct {
	dbdir string
	// BlockSize is the size of each block in the file in bytes.
	// In a real database system, this value should be set to the the block
	// size defined by the operating system.
	BlockSize int
	IsNew     bool
	openFiles map[string]*os.File
	mu        sync.Mutex
}

// NewFileMgr creates a new FileMgr with the given directory name and blocksize.
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

// Close closes all open files
func (fm *FileMgr) Close() {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	for _, f := range fm.openFiles {
		f.Close()
	}
}

// Read reads the contents of the specified block into the specified page
func (fm *FileMgr) Read(blk BlockID, p *Page) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	f, err := fm.getFile(blk.Filename)
	if err != nil {
		return err
	}

	offset := int64(blk.Blknum) * int64(fm.BlockSize)
	if _, err = f.ReadAt(p.buf, offset); err != nil {
		if err != io.EOF {
			return fmt.Errorf("cannot read block %s: %w", blk, err)
		}
	}

	// note: if we read less bytes than the size of the page buffer, it's ok

	return nil
}

// Write writes the contents of a page to the specified block
func (fm *FileMgr) Write(blk BlockID, p *Page) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	f, err := fm.getFile(blk.Filename)
	if err != nil {
		return err
	}

	offset := int64(blk.Blknum) * int64(fm.BlockSize)
	if _, err := f.WriteAt(p.buf, offset); err != nil {
		return fmt.Errorf("cannot write block %s: %w", blk, err)
	}

	return nil
}

// Append seeks to the end of the file and writes an empty array of bytes,
// extending the file. It returns the block id of the newly created block
// within the file.
func (fm *FileMgr) Append(filename string) (BlockID, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	f, err := fm.getFile(filename)
	if err != nil {
		return BlockID{}, err
	}

	info, err := f.Stat()
	if err != nil {
		return BlockID{}, fmt.Errorf("cannot stat file %s: %w", filename, err)
	}

	newblknum := int(info.Size() / int64(fm.BlockSize))
	buf := make([]byte, fm.BlockSize) // an empty block of data
	offset := int64(newblknum * fm.BlockSize)
	if _, err := f.WriteAt(buf, offset); err != nil {
		return BlockID{}, fmt.Errorf("cannot write to file %s: %w", filename, err)
	}

	return NewBlockID(filename, newblknum), nil
}

// Length returns the number of blocks in the specified file
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

// getFile gets a file from the list of open files or creates a new one if it
// doesn't exist.
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
