package web

import (
	"errors"
	"io"
	"log"
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
		laddr := agent.LeaderAddr()
		if laddr == "" {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			http.Redirect(w, r, agent.LeaderAddr(), http.StatusTemporaryRedirect)
		}
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
		if 0 < lockHandlerID {
			w.WriteHeader(http.StatusConflict)
			return
		}
		logEntry := agent.LogEntry{
			LockHolderID: int32(lockRequestedID),
		}
		err = agent.AppendLog(&logEntry)
		if errors.Is(err, agent.DemotedToFollower) {
			http.Redirect(w, r, agent.LeaderAddr(), http.StatusTemporaryRedirect)
			return
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
}

func UnlockHandler(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()

	if !agent.IsLeader() {
		laddr := agent.LeaderAddr()
		if laddr == "" {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			http.Redirect(w, r, agent.LeaderAddr(), http.StatusTemporaryRedirect)
		}
		return
	}

	if r.Method != http.MethodPut {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()
	unlockRequestedID, err := strconv.ParseInt(string(body), 10, 32)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	lockHandlerID := agent.LockHolderID()
	if unlockRequestedID < 0 {
		log.Printf("Current lock holder's ID is %d, but unlock requested for ID %d.",
			lockHandlerID, unlockRequestedID)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if unlockRequestedID != int64(lockHandlerID) {
		log.Printf("Current lock holder's ID is %d, but unlock requested for ID %d.",
			lockHandlerID, unlockRequestedID)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if unlockRequestedID < 0 {
		log.Printf("Unlock requested for an invalid ID %d.", unlockRequestedID)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	logEntry := agent.LogEntry{
		LockHolderID: agent.InvalidLockHolderID,
	}
	err = agent.AppendLog(&logEntry)
	if errors.Is(err, agent.DemotedToFollower) {
		http.Redirect(w, r, agent.LeaderAddr(), http.StatusTemporaryRedirect)
		return
	}
}