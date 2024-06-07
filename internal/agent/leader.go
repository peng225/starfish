package agent

import (
	"context"
	"errors"
	"log"
	"time"

	sfrpc "github.com/peng225/starfish/internal/rpc"
)

type VolatileLeaderState struct {
	nextIndex  []int64
	matchIndex []int64
}

type sendLogRequest struct {
	ctx        context.Context
	cancel     context.CancelCauseFunc
	logEntries []LogEntry
	errCh      chan error
}

const (
	queueLength = 100000
)

var (
	vlstate           VolatileLeaderState
	DemotedToFollower error

	sendLogQueue []chan sendLogRequest
)

func init() {
	vlstate = VolatileLeaderState{
		// FIXME: The number of agents is fixed to 3.
		nextIndex:  make([]int64, 3),
		matchIndex: make([]int64, 3),
	}
	DemotedToFollower = errors.New("demoted to the follower")
	sendLogQueue = make([]chan sendLogRequest, queueLength)
}

func AppendLog(logEntry *LogEntry) error {
	logEntry.Term = pstate.currentTerm
	pstate.log = append(pstate.log, *logEntry)
	// TODO: save to disk

	errCh := sendLogToDaemon(logEntry)
	for i := 0; i < len(addrs)/2+1; i++ {
		err := <-errCh
		if err != nil {
			return err
		}
	}

	vstate.commitIndex++
	mstate.LockHolderID = logEntry.LockHolderID
	vstate.lastApplied++
	return nil
}

func sendLogToDaemon(logEntry *LogEntry) chan error {
	errCh := make(chan error, len(addrs)-1)
	ctx, cancel := context.WithCancelCause(context.Background())
	for i := range addrs {
		i := i
		if i == int(vstate.id) {
			continue
		}
		sendLogQueue[i] <- sendLogRequest{
			ctx:        ctx,
			cancel:     cancel,
			logEntries: []LogEntry{*logEntry},
			errCh:      errCh,
		}
	}
	return errCh
}

func sendLog(ctx context.Context, destID int32, logEntries []LogEntry) error {
	entries := make([]*sfrpc.LogEntry, 0)
	for _, e := range logEntries {
		entries = append(entries, &sfrpc.LogEntry{
			LockHolderID: e.LockHolderID,
		})
	}
	reply, err := rpcClients[destID].AppendEntries(ctx, &sfrpc.AppendEntriesRequest{
		Term:         pstate.currentTerm,
		LeaderID:     vstate.id,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      entries,
		LeaderCommit: vstate.commitIndex,
	})
	if err != nil {
		log.Printf("AppendEntries RPC for %s failed. err: %s", addrs[destID], err.Error())
		return err
	}
	if !reply.Success {
		log.Printf("AppendEntries RPC for %s failed.", addrs[destID])
		if reply.Term > pstate.currentTerm {
			transitionToFollower(reply.Term)
			return DemotedToFollower
		}
	}
	return nil
}

func sendLogDaemon(destID int32) {
	for {
		req := <-sendLogQueue[destID]
		for {
			err := sendLog(req.ctx, destID, req.logEntries)
			if err != nil {
				if errors.Is(err, DemotedToFollower) {
					log.Println("Demoted to the follower.")
					req.cancel(DemotedToFollower)
					req.errCh <- err
					return
				}
				time.Sleep(100 * time.Millisecond)
				continue
			}
			req.errCh <- nil
			break
		}
	}
}

func sendHeartBeat() {
	ticker := time.NewTicker(time.Second)
OutMost:
	for {
		<-ticker.C
		// TODO: we may need a lock to check the current role.
		if vstate.role != Leader {
			break
		}
		for i := range addrs {
			err := sendLog(context.Background(), int32(i), nil)
			if errors.Is(err, DemotedToFollower) {
				log.Println("Demoted to the follower.")
				break OutMost
			}
		}
	}
}
