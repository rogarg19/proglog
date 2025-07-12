package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
	width = uint64(len(write) + lenWidth)
)

func TestStoreAppendReadClose(t *testing.T) {
	f, err := os.CreateTemp("", "store_temp_file")
	require.NoError(t, err)
	defer f.Close()

	store, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, store)
	testRead(t, store)
	testReadAt(t, store)
	testClose(t, store)

	//test new store after close/crash
	store, err = newStore(f)
	require.NoError(t, err)
	err = store.Close()
	require.NoError(t, err)

}

func testAppend(t *testing.T, store *store) {
	t.Helper()

	for i := uint64(1); i < 4; i++ {
		w, pos, err := store.Append(write)
		require.NoError(t, err)
		require.Equal(t, w+pos, width*i)
	}
}

func testRead(t *testing.T, store *store) {
	t.Helper()
	var pos uint64

	for i := uint64(1); i < 4; i++ {
		message, err := store.Read(pos)
		require.NoError(t, err)
		require.Equal(t, message, write)
		pos += width
	}
}

func testReadAt(t *testing.T, store *store) {
	t.Helper()

	// Write a record to the store
	_, pos, err := store.Append(write)
	require.NoError(t, err)

	// Create a buffer to read into
	b := make([]byte, width)

	// Read the record at the given position (convert pos to int64)
	n, err := store.ReadAt(b, int64(pos))
	require.NoError(t, err)
	require.Equal(t, int(width), n)

	// Decode the length prefix
	size := enc.Uint64(b[:lenWidth])
	// The message should match the written data
	require.Equal(t, write, b[lenWidth:int(size)+lenWidth])
}

func testClose(t *testing.T, store *store) {
	t.Helper()

	err := store.Close()
	require.NoError(t, err)
}
