package store

import (
	"os"
	"testing"

	"github.com/peng225/starfish/internal/agent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMustNewFileStore(t *testing.T) {
	fileName := "/tmp/test.bin"
	_ = os.Remove(fileName)
	fs := MustNewFileStore(fileName)
	require.Equal(t, int64(0), fs.currentTerm)
	require.Equal(t, agent.InvalidAgentID, fs.votedFor)
	require.Equal(t, int64(0), fs.LogSize())
	_, err := os.Stat(fileName)
	require.NoError(t, err)

	fs = MustNewFileStore(fileName)
	assert.Equal(t, int64(0), fs.currentTerm)
	assert.Equal(t, agent.InvalidAgentID, fs.votedFor)
	assert.Equal(t, int64(0), fs.LogSize())
}

func TestPutAndGet(t *testing.T) {
	fileName := "/tmp/test.bin"
	_ = os.Remove(fileName)
	fs := MustNewFileStore(fileName)

	fs.PutCurrentTerm(10)
	assert.Equal(t, int64(10), fs.currentTerm)

	fs.PutVotedFor(2)
	assert.Equal(t, int32(2), fs.votedFor)

	for i := 0; i < 3; i++ {
		i := i
		fs.AppendLog(&agent.LogEntry{
			Term:         4,
			LockHolderID: int32(i),
		})
	}
	assert.Equal(t, int64(3), fs.LogSize())
	for i := 0; i < int(fs.LogSize()); i++ {
		i := i
		assert.Equal(t, int64(4), fs.LogEntry(int64(i)).Term)
		assert.Equal(t, int32(i), fs.LogEntry(int64(i)).LockHolderID)
	}
}

func TestPutAndReloadFromFile(t *testing.T) {
	fileName := "/tmp/test.bin"
	_ = os.Remove(fileName)
	fs := MustNewFileStore(fileName)

	fs.PutCurrentTerm(10)
	fs.PutVotedFor(2)
	for i := 0; i < 5; i++ {
		i := i
		fs.AppendLog(&agent.LogEntry{
			Term:         4,
			LockHolderID: int32(i),
		})
	}

	fs = MustNewFileStore(fileName)
	assert.Equal(t, int64(10), fs.currentTerm)
	assert.Equal(t, int32(2), fs.votedFor)
	assert.Equal(t, int64(5), fs.LogSize())
	for i := 0; i < int(fs.LogSize()); i++ {
		i := i
		assert.Equal(t, int64(4), fs.LogEntry(int64(i)).Term)
		assert.Equal(t, int32(i), fs.LogEntry(int64(i)).LockHolderID)
	}
}

func TestCutOffLogTail(t *testing.T) {
	fileName := "/tmp/test.bin"
	_ = os.Remove(fileName)
	fs := MustNewFileStore(fileName)

	for i := 0; i < 5; i++ {
		i := i
		fs.AppendLog(&agent.LogEntry{
			Term:         4,
			LockHolderID: int32(i),
		})
	}
	fs.CutOffLogTail(3)
	assert.Equal(t, int64(3), fs.LogSize())
	for i := 0; i < int(fs.LogSize()); i++ {
		i := i
		assert.Equal(t, int64(4), fs.LogEntry(int64(i)).Term)
		assert.Equal(t, int32(i), fs.LogEntry(int64(i)).LockHolderID)
	}

	fs = MustNewFileStore(fileName)
	assert.Equal(t, int64(3), fs.LogSize())
	for i := 0; i < int(fs.LogSize()); i++ {
		i := i
		assert.Equal(t, int64(4), fs.LogEntry(int64(i)).Term)
		assert.Equal(t, int32(i), fs.LogEntry(int64(i)).LockHolderID)
	}
}

func TestCutOffAndPut(t *testing.T) {
	fileName := "/tmp/test.bin"
	_ = os.Remove(fileName)
	fs := MustNewFileStore(fileName)

	for i := 0; i < 5; i++ {
		i := i
		fs.AppendLog(&agent.LogEntry{
			Term:         4,
			LockHolderID: int32(i),
		})
	}
	fs.CutOffLogTail(3)
	fs.AppendLog(&agent.LogEntry{
		Term:         4,
		LockHolderID: int32(50),
	})
	assert.Equal(t, int64(4), fs.LogSize())
	for i := 0; i < int(fs.LogSize())-1; i++ {
		i := i
		assert.Equal(t, int64(4), fs.LogEntry(int64(i)).Term)
		assert.Equal(t, int32(i), fs.LogEntry(int64(i)).LockHolderID)
	}
	assert.Equal(t, int64(4), fs.LogEntry(int64(3)).Term)
	assert.Equal(t, int32(50), fs.LogEntry(int64(3)).LockHolderID)

	fs = MustNewFileStore(fileName)
	assert.Equal(t, int64(4), fs.LogSize())
	for i := 0; i < int(fs.LogSize())-1; i++ {
		i := i
		assert.Equal(t, int64(4), fs.LogEntry(int64(i)).Term)
		assert.Equal(t, int32(i), fs.LogEntry(int64(i)).LockHolderID)
	}
	assert.Equal(t, int64(4), fs.LogEntry(int64(3)).Term)
	assert.Equal(t, int32(50), fs.LogEntry(int64(3)).LockHolderID)
}
