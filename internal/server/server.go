package server

import (
	"io"
	"net/http"
	"strconv"
	"sync"

	"github.com/peng225/starfish/internal/agent"
)

var mu sync.Mutex

func LockHandler(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()

	if !agent.IsLeader() {
		w.WriteHeader(http.StatusPermanentRedirect)
		w.Header().Add("Location", agent.LeaderAddr())
		return
	}

	lockHandlerID := agent.LockHolderID()
	switch r.Method {
	case http.MethodGet:
		lhIDByte := []byte(strconv.FormatInt(int64(lockHandlerID), 10))
		written := 0
		for len(lhIDByte) != written {
			n, err := w.Write(lhIDByte[written:])
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
			}
			written += n
		}
	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()
		lockRequestedID, err := strconv.ParseInt(string(body), 10, 32)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if lockRequestedID == int64(lockHandlerID) {
			// Already has a lock.
			break
		}
		if lockHandlerID != -1 {
			w.WriteHeader(http.StatusConflict)
			return
		}
		logEntries := make([]agent.LogEntry, 1)
		logEntries[0] = agent.LogEntry{
			LockHolderID: int32(lockRequestedID),
		}
		agent.SendLog(logEntries)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
}
