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
	ctx      context.Context
	cancel   context.CancelCauseFunc
	endIndex int64
	errCh    chan error
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
	DemotedToFollower = errors.New("demoted to the follower")
	LogMismatch = errors.New("log mismatch")
}

func initLeader() {
	vlstate = VolatileLeaderState{
		nextIndex:  make([]int64, len(grpcEndpoints)),
		matchIndex: make([]int64, len(grpcEndpoints)),
	}

	sendLogQueues = make([]chan sendLogRequest, len(grpcEndpoints))
	for i := 0; i < len(sendLogQueues); i++ {
		sendLogQueues[i] = make(chan sendLogRequest, queueLength)
	}
}

func initLeaderOnPromotion() {
	for i := 0; i < len(vlstate.nextIndex); i++ {
		vlstate.nextIndex[i] = int64(len(pstate.log))
	}
	for i := 0; i < len(vlstate.matchIndex); i++ {
		vlstate.matchIndex[i] = -1
	}

	broadcastHeartBeat()
}

func AppendLog(logEntry *LogEntry) error {
	log.Println("AppendLog start.")
	defer log.Println("AppendLog end.")
	logEntry.Term = pstate.currentTerm
	pstate.log = append(pstate.log, *logEntry)
	// TODO: save to disk

	errCh := broadcastToLogSenderDaemons(int64(len(pstate.log)))
	for i := 0; i < len(grpcEndpoints)/2+1; i++ {
		err := <-errCh
		if err != nil {
			return err
		}
	}

	updateCommitIndex(vstate.commitIndex + 1)
	return nil
}

func broadcastToLogSenderDaemons(endIndex int64) chan error {
	errCh := make(chan error, len(grpcEndpoints)-1)
	ctx, cancel := context.WithCancelCause(context.Background())
	for i := range grpcEndpoints {
		if i == int(vstate.id) {
			continue
		}
		sendLogQueues[i] <- sendLogRequest{
			ctx:      ctx,
			cancel:   cancel,
			endIndex: endIndex,
			errCh:    errCh,
		}
	}
	return errCh
}

func logSenderDaemon(destID int32) {
	for {
		req := <-sendLogQueues[destID]
		err := sendLogWithRetry(req.ctx, req.cancel, destID,
			req.endIndex, req.errCh)
		if err != nil {
			if !errors.Is(err, DemotedToFollower) {
				log.Fatalf("Fatal error: %s", err)
			}
		}
	}
}

func sendLogWithRetry(ctx context.Context, cancel context.CancelCauseFunc, destID int32,
	endIndex int64, errCh chan error) error {
	for {
		err := sendLog(ctx, destID, endIndex-vlstate.nextIndex[destID])
		if err != nil {
			if errors.Is(err, DemotedToFollower) {
				log.Println("Demoted to the follower.")
				cancel(DemotedToFollower)
				errCh <- err
				return err
			} else if errors.Is(err, LogMismatch) {
				vlstate.nextIndex[destID]--
			} else {
				time.Sleep(200 * time.Millisecond)
			}
			continue
		}
		if endIndex <= vlstate.nextIndex[destID] {
			break
		}
	}
	errCh <- nil
	return nil
}

// Send log entries to the destination denoted by 'destID.'
// 'entryCount' represents the number of entries to be sent.
// To-be-sent entries begin with the 'vlstate.nextIndex[destID]'-th log.
func sendLog(ctx context.Context, destID int32, entryCount int64) error {
	if entryCount < 0 {
		log.Fatalf("entryCount must not be a negative number. entryCount: %d",
			entryCount)
	}
	entries := make([]*sfrpc.LogEntry, 0)
	for _, e := range pstate.log[vlstate.nextIndex[destID] : vlstate.nextIndex[destID]+entryCount] {
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
		log.Printf("AppendEntries RPC for %s failed. err: %s", grpcEndpoints[destID], err.Error())
		return err
	}
	if !reply.Success {
		log.Printf("AppendEntries RPC for %s failed.", grpcEndpoints[destID])
		if reply.Term > pstate.currentTerm {
			transitionToFollower()
			pstate.currentTerm = reply.Term
			return DemotedToFollower
		}
		return LogMismatch
	}
	vlstate.nextIndex[destID] += entryCount
	return nil
}

func broadcastHeartBeat() {
	errCh := broadcastToLogSenderDaemons(int64(len(pstate.log)))
	for i := 0; i < len(grpcEndpoints)/2+1; i++ {
		err := <-errCh
		if err != nil {
			if errors.Is(err, DemotedToFollower) {
				break
			}
			log.Fatalf("Unexpected heartbeat error. err: %s", err)
		}
	}
}

func heartBeatDaemon() {
	ticker := time.NewTicker(time.Second)
	for {
		<-ticker.C
		if vstate.role != Leader {
			break
		}
		broadcastHeartBeat()
	}
}
