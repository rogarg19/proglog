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

func (store *store) Append(message []byte) (w uint64, pos uint64, err error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	if err := binary.Write(store.buf, enc, len(message)); err != nil {
		return 0, 0, fmt.Errorf("failed to write length of message. Error %s", err)
	}

	n, err := store.buf.Write(message)

	if err != nil {
		return 0, 0, fmt.Errorf("failed to append message. Error %s", err)
	}

	n += lenWidth

	store.size += uint64(n)
}

func (store *store) Read(pos uint64) (message []byte, err error) {

}
