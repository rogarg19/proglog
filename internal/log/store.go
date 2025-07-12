package log

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

// Creates a new disk store with passed file
func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())

	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Appends message bytes to the store via buffer. Returns total bytes written, pos of appended record and error if any
func (store *store) Append(message []byte) (w uint64, pos uint64, err error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	pos = store.size

	if err := binary.Write(store.buf, enc, uint64(len(message))); err != nil {
		return 0, 0, fmt.Errorf("failed to write length of message. Error %w", err)
	}

	n, err := store.buf.Write(message)

	if err != nil {
		return 0, 0, fmt.Errorf("failed to append message. Error %w", err)
	}

	n += lenWidth
	store.size += uint64(n)

	return uint64(n), pos, nil
}

// Reads message from store stored at a specific postion
func (store *store) Read(pos uint64) ([]byte, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	// flush the remaining buffer to file if not done already
	if err := store.buf.Flush(); err != nil {
		return nil, fmt.Errorf("error flushing buffer to file. Error- %w", err)
	}

	size := make([]byte, lenWidth)

	if _, err := store.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	b := make([]byte, enc.Uint64(size))
	if _, err := store.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

// Reads message from store stored at a specific postion
func (store *store) ReadAt(message []byte, offset int64) (int, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	// flush the remaining buffer to file if not done already
	if err := store.buf.Flush(); err != nil {
		return 0, fmt.Errorf("error flushing buffer to file. Error- %w", err)
	}

	return store.File.ReadAt(message, offset)
}

// Closes the store, flushing any remaining buffer
func (store *store) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()

	// flush the remaining buffer to file if not done already
	if err := store.buf.Flush(); err != nil {
		return err
	}

	store.File.Close()

	return nil
}
