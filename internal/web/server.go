package web

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/peng225/starfish/internal/agent"
	"github.com/peng225/starfish/internal/gmutex"
)

var (
	webEndpoints []string
)

func Init(we []string) {
	webEndpoints = we
}

func LockHandler(w http.ResponseWriter, r *http.Request) {
	slog.Debug("LockHandler start.")
	defer slog.Debug("LockHandler end.")
	gmutex.Lock()
	defer gmutex.Unlock()

	if !agent.IsLeader() {
		lid := agent.LeaderID()
		if lid == agent.InvalidAgentID {
			slog.Warn("Currently, there is no leader.")
			w.Header().Add("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			slog.Debug("I am not a leader.",
				slog.Int("leaderID", int(lid)))
			http.Redirect(w, r, webEndpoints[lid]+"/lock", http.StatusTemporaryRedirect)
		}
		return
	}

	i := 0
	for agent.PendingApplyLogExist() {
		if i == 3 {
			slog.Warn("Pending requests exist.")
			w.Header().Add("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		i++
		time.Sleep(10 * time.Millisecond)
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
		if 0 <= lockHandlerID {
			w.WriteHeader(http.StatusConflict)
			return
		}
		logEntry := agent.LogEntry{
			LockHolderID: int32(lockRequestedID),
		}
		err = agent.AppendLog(&logEntry)
		if err != nil && errors.Is(err, agent.DemotedToFollower) {
			lid := agent.LeaderID()
			http.Redirect(w, r, webEndpoints[lid]+"/lock", http.StatusTemporaryRedirect)
			return
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
}

func UnlockHandler(w http.ResponseWriter, r *http.Request) {
	slog.Debug("UnlockHandler start.")
	defer slog.Debug("UnlockHandler end.")
	gmutex.Lock()
	defer gmutex.Unlock()

	if !agent.IsLeader() {
		lid := agent.LeaderID()
		if lid == agent.InvalidAgentID {
			slog.Warn("Currently, there is no leader.")
			w.Header().Add("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			slog.Debug("I am not a leader.",
				slog.Int("leaderID", int(lid)))
			http.Redirect(w, r, webEndpoints[lid]+"/unlock", http.StatusTemporaryRedirect)
		}
		return
	}

	if r.Method != http.MethodPut {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	i := 0
	for agent.PendingApplyLogExist() {
		if i == 3 {
			slog.Warn("Pending requests exist.")
			w.Header().Add("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		i++
		time.Sleep(10 * time.Millisecond)
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
		slog.Error(fmt.Sprintf("Current lock holder's ID is %d, but unlock requested for ID %d.",
			lockHandlerID, unlockRequestedID))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if unlockRequestedID != int64(lockHandlerID) {
		slog.Error(fmt.Sprintf("Current lock holder's ID is %d, but unlock requested for ID %d.",
			lockHandlerID, unlockRequestedID))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if unlockRequestedID < 0 {
		slog.Error("Unlock requested for an invalid ID.",
			slog.Int("ID", int(unlockRequestedID)))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	logEntry := agent.LogEntry{
		LockHolderID: agent.InvalidLockHolderID,
	}
	err = agent.AppendLog(&logEntry)
	if err != nil && errors.Is(err, agent.DemotedToFollower) {
		lid := agent.LeaderID()
		http.Redirect(w, r, webEndpoints[lid]+"/unlock", http.StatusTemporaryRedirect)
		return
	}
}
