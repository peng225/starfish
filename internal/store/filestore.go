package store

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"reflect"
	"sync"

	"github.com/peng225/starfish/internal/agent"
)

const (
	markerOffset   int64  = 0
	termOffset     int64  = 8
	votedForOffset int64  = 16
	logSizeOffset  int64  = 24
	logOffset      int64  = 32
	marker         uint64 = 0x6873696672617473
)

type FileStore struct {
	rwm         sync.RWMutex
	file        *os.File
	currentTerm int64
	votedFor    int32
	log         []*agent.LogEntry
}

var _ agent.PersistentStore = (*FileStore)(nil)

// File binary format:
// 0x0000  <--------- marker(8b) ------------->
// 0x0008  <--------- term(8b) --------------->
// 0x0010  <- votedFor(4b) -><- empty(4b) ---->
// 0x0018  <--------- log size(8b) ----------->
// 0x0020  <--------- log entry[0] -- ...

func MustNewFileStore(fileName string) *FileStore {
	var fs FileStore
	fs.log = make([]*agent.LogEntry, 0)

	_, err := os.Stat(fileName)
	if err != nil {
		slog.Info("File not found. Create a new one.",
			slog.String("fileName", fileName))
		fs.log = make([]*agent.LogEntry, 0)
		fs.file, err = os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0664)
		if err != nil {
			slog.Error("OpenFile failed.",
				slog.String("err", err.Error()))
			os.Exit(1)
		}
		fs.putMarker()
		fs.PutCurrentTerm(0)
		fs.PutVotedFor(agent.InvalidAgentID)
		fs.putLogSize()
		return &fs
	}

	fs.file, err = os.OpenFile(fileName, os.O_RDWR|os.O_SYNC, 0664)
	if err != nil {
		slog.Error("OpenFile failed",
			slog.String("err", err.Error()))
		os.Exit(1)
	}

	var m uint64
	err = binary.Read(fs.file, binary.LittleEndian, &m)
	if err != nil {
		slog.Error("binary.Read marker failed.",
			slog.String("err", err.Error()))
		os.Exit(1)
	}
	if m != marker {
		slog.Error("Corrupted file marker.",
			slog.String("expected", fmt.Sprintf("%#x", marker)),
			slog.String("actaual", fmt.Sprintf("%#x", m)))
		os.Exit(1)
	}
	err = binary.Read(fs.file, binary.LittleEndian, &fs.currentTerm)
	if err != nil {
		slog.Error("binary.Read currentTerm failed.",
			slog.String("err", err.Error()))
		os.Exit(1)
	}
	err = binary.Read(fs.file, binary.LittleEndian, &fs.votedFor)
	if err != nil {
		slog.Error("binary.Read votedFor failed.",
			slog.String("err", err.Error()))
		os.Exit(1)
	}
	_, err = fs.file.Seek(4, io.SeekCurrent)
	if err != nil {
		slog.Error("Seek failed.",
			slog.String("err", err.Error()))
		os.Exit(1)
	}
	var logSize int64
	err = binary.Read(fs.file, binary.LittleEndian, &logSize)
	if err != nil {
		slog.Error("binary.Read logSize failed.",
			slog.String("err", err.Error()))
		os.Exit(1)
	}
	for i := int64(0); i < logSize; i++ {
		var entry agent.LogEntry
		err = binary.Read(fs.file, binary.LittleEndian, &entry)
		if err != nil {
			slog.Error(fmt.Sprintf("binary.Read %d-th entry failed.", i),
				slog.String("err", err.Error()))
			os.Exit(1)
		}
		fs.log = append(fs.log, &entry)
	}

	return &fs
}

func (fs *FileStore) putMarker() {
	fs.rwm.Lock()
	defer fs.rwm.Unlock()

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, marker)
	_, err := fs.file.WriteAt(buf, markerOffset)
	if err != nil {
		slog.Error("Failed to write marker.",
			slog.String("err", err.Error()))
		os.Exit(1)
	}
}

func (fs *FileStore) CurrentTerm() int64 {
	fs.rwm.RLock()
	defer fs.rwm.RUnlock()
	return fs.currentTerm
}

func (fs *FileStore) PutCurrentTerm(term int64) {
	fs.rwm.Lock()
	defer fs.rwm.Unlock()

	fs.currentTerm = term
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(term))
	_, err := fs.file.WriteAt(buf, termOffset)
	if err != nil {
		slog.Error("Failed to write offset.",
			slog.String("err", err.Error()))
		os.Exit(1)
	}
}

func (fs *FileStore) VotedFor() int32 {
	fs.rwm.RLock()
	defer fs.rwm.RUnlock()
	return fs.votedFor
}

func (fs *FileStore) PutVotedFor(vf int32) {
	fs.rwm.Lock()
	defer fs.rwm.Unlock()

	fs.votedFor = vf
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(vf))
	_, err := fs.file.WriteAt(buf, votedForOffset)
	if err != nil {
		slog.Error("Failed to write votedFor.",
			slog.String("err", err.Error()))
		os.Exit(1)
	}
}

func (fs *FileStore) LogEntry(i int64) *agent.LogEntry {
	fs.rwm.RLock()
	defer fs.rwm.RUnlock()
	if i >= int64(len(fs.log)) {
		return nil
	}
	return fs.log[i]
}

func (fs *FileStore) LogSize() int64 {
	fs.rwm.RLock()
	defer fs.rwm.RUnlock()
	return int64(len(fs.log))
}

func (fs *FileStore) putLogSize() {
	fs.rwm.Lock()
	defer fs.rwm.Unlock()

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(len(fs.log)))
	_, err := fs.file.WriteAt(buf, logSizeOffset)
	if err != nil {
		slog.Error("Failed to write log size.",
			slog.String("err", err.Error()))
		os.Exit(1)
	}
}

func (fs *FileStore) AppendLog(e *agent.LogEntry) {
	fs.rwm.Lock()
	defer fs.rwm.Unlock()

	offset := logOffset + int64(len(fs.log))*int64(reflect.TypeOf(*e).Size())
	fs.log = append(fs.log, e)
	_, err := fs.file.Seek(offset, io.SeekStart)
	if err != nil {
		slog.Error("Seek failed.",
			slog.String("err", err.Error()))
		os.Exit(1)
	}
	err = binary.Write(fs.file, binary.LittleEndian, e)
	if err != nil {
		slog.Error("Write failed.",
			slog.String("err", err.Error()))
		os.Exit(1)
	}

	// Update the log size on disk.
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(len(fs.log)))
	_, err = fs.file.WriteAt(buf, logSizeOffset)
	if err != nil {
		slog.Error("Failed to write log size.",
			slog.String("err", err.Error()))
		os.Exit(1)
	}
}

func (fs *FileStore) CutOffLogTail(from int64) {
	fs.rwm.Lock()
	defer fs.rwm.Unlock()
	fs.log = fs.log[:from]
	// Ignore the deleted entries on disk.
	// Update the log size on disk.
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(len(fs.log)))
	_, err := fs.file.WriteAt(buf, logSizeOffset)
	if err != nil {
		slog.Error("Failed to write log size.",
			slog.String("err", err.Error()))
		os.Exit(1)
	}
}
