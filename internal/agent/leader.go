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
	ctx    context.Context
	cancel context.CancelCauseFunc
	errCh  chan error
}

const (
	queueLength = 100000
)

var (
	vlstate           VolatileLeaderState
	DemotedToFollower error
	LogMismatch       error

	sendLogQueues []chan sendLogRequest
)

func init() {
	vlstate = VolatileLeaderState{
		// FIXME: The number of agents is fixed to 3.
		nextIndex:  make([]int64, 3),
		matchIndex: make([]int64, 3),
	}
	for i := 0; i < len(vlstate.nextIndex); i++ {
		vlstate.nextIndex[i] = int64(len(pstate.log))
	}
	for i := 0; i < len(vlstate.matchIndex); i++ {
		vlstate.matchIndex[i] = -1
	}
	DemotedToFollower = errors.New("demoted to the follower")
	LogMismatch = errors.New("log mismatch")
	sendLogQueues = make([]chan sendLogRequest, len(addrs))
	for i := 0; i < len(sendLogQueues); i++ {
		sendLogQueues[i] = make(chan sendLogRequest, queueLength)
	}
}

func AppendLog(logEntry *LogEntry) error {
	log.Println("AppendLog start.")
	defer log.Println("AppendLog end.")
	logEntry.Term = pstate.currentTerm
	pstate.log = append(pstate.log, *logEntry)
	// TODO: save to disk

	errCh := sendLogToDaemon()
	for i := 0; i < len(addrs)/2+1; i++ {
		err := <-errCh
		if err != nil {
			return err
		}
	}

	vstate.commitIndex++
	sm.LockHolderID = logEntry.LockHolderID
	vstate.lastApplied++
	return nil
}

func sendLogToDaemon() chan error {
	errCh := make(chan error, len(addrs)-1)
	ctx, cancel := context.WithCancelCause(context.Background())
	for i := range addrs {
		if i == int(vstate.id) {
			continue
		}
		sendLogQueues[i] <- sendLogRequest{
			ctx:    ctx,
			cancel: cancel,
			errCh:  errCh,
		}
	}
	return errCh
}

func sendLogDaemon(destID int32) {
	for {
		req := <-sendLogQueues[destID]
		log.Println("sendLogDaemon kicked.")
		err := sendLogWithRetry(req.ctx, req.cancel, destID, req.errCh)
		if err != nil {
			if !errors.Is(err, DemotedToFollower) {
				log.Fatalf("Fatal error: %s", err)
			}
		}
	}
}

func sendLogWithRetry(ctx context.Context, cancel context.CancelCauseFunc, destID int32, errCh chan error) error {
	originalNextIndex := vlstate.nextIndex[destID]
	for vlstate.nextIndex[destID] <= originalNextIndex {
		err := sendLog(ctx, destID,
			[]LogEntry{pstate.log[vlstate.nextIndex[destID]]})
		if err != nil {
			if errors.Is(err, DemotedToFollower) {
				log.Println("Demoted to the follower.")
				cancel(DemotedToFollower)
				errCh <- err
				return err
			} else if errors.Is(err, LogMismatch) {
				vlstate.nextIndex[destID]--
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		vlstate.nextIndex[destID]++
	}
	errCh <- nil
	return nil
}

func sendLog(ctx context.Context, destID int32, logEntries []LogEntry) error {
	entries := make([]*sfrpc.LogEntry, 0)
	for _, e := range logEntries {
		entries = append(entries, &sfrpc.LogEntry{
			LockHolderID: e.LockHolderID,
		})
	}
	plt := int64(-1)
	if vlstate.nextIndex[destID] > 0 {
		plt = pstate.log[vlstate.nextIndex[destID]-1].Term
	}
	reply, err := rpcClients[destID].AppendEntries(ctx, &sfrpc.AppendEntriesRequest{
		Term:         pstate.currentTerm,
		LeaderID:     vstate.id,
		PrevLogIndex: vlstate.nextIndex[destID] - 1,
		PrevLogTerm:  plt,
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
			transitionToFollower()
			pstate.currentTerm = reply.Term
			return DemotedToFollower
		}
		return LogMismatch
	}
	return nil
}

func sendHeartBeat() error {
	for i := range addrs {
		if i == int(vstate.id) {
			continue
		}
		err := sendLog(context.Background(), int32(i), nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func heartBeatDaemon() {
	ticker := time.NewTicker(time.Second)
	for {
		<-ticker.C
		if vstate.role != Leader {
			break
		}
		err := sendHeartBeat()
		if err != nil && errors.Is(err, DemotedToFollower) {
			log.Println("Demoted to the follower.")
			break
		}
	}
}
