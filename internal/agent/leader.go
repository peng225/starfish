package agent

import (
	"context"
	"errors"
	"log/slog"
	"os"
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
		vlstate.nextIndex[i] = pstore.LogSize()
	}
	for i := 0; i < len(vlstate.matchIndex); i++ {
		vlstate.matchIndex[i] = -1
	}
}

func AppendLog(logEntry *LogEntry) error {
	slog.Info("AppendLog start.")
	defer slog.Info("AppendLog end.")
	logEntry.Term = pstore.CurrentTerm()
	pstore.AppendLog(logEntry)

	errCh := broadcastToLogSenderDaemons(pstore.LogSize())
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
				slog.Error("Fatal error.",
					slog.String("err", err.Error()))
				os.Exit(1)
			}
		}
	}
}

func sendLogWithRetry(ctx context.Context, cancel context.CancelCauseFunc,
	destID int32, endIndex int64, errCh chan error) error {
	for {
		err := sendLog(ctx, destID, endIndex-vlstate.nextIndex[destID])
		if err != nil {
			if errors.Is(err, DemotedToFollower) {
				slog.Info("Demoted to the follower.")
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
		slog.Error("entryCount must not be a negative number.",
			slog.Int64("entryCount", entryCount))
		os.Exit(1)
	}
	entries := make([]*sfrpc.LogEntry, 0)
	for i := vlstate.nextIndex[destID]; i < vlstate.nextIndex[destID]+entryCount; i++ {
		entries = append(entries, &sfrpc.LogEntry{
			LockHolderID: pstore.LogEntry(i).LockHolderID,
		})
	}
	plt := int64(-1)
	if vlstate.nextIndex[destID] > 0 {
		plt = pstore.LogEntry(vlstate.nextIndex[destID] - 1).Term
	}
	reply, err := rpcClients[destID].AppendEntries(ctx, &sfrpc.AppendEntriesRequest{
		Term:         pstore.CurrentTerm(),
		LeaderID:     vstate.id,
		PrevLogIndex: vlstate.nextIndex[destID] - 1,
		PrevLogTerm:  plt,
		Entries:      entries,
		LeaderCommit: vstate.commitIndex,
	})
	if err != nil {
		slog.Error("AppendEntries RPC failed.",
			slog.String("dest", grpcEndpoints[destID]),
			slog.String("err", err.Error()))
		return err
	}
	if !reply.Success {
		slog.Error("AppendEntries RPC failed.",
			slog.String("dest", grpcEndpoints[destID]))
		if reply.Term > pstore.CurrentTerm() {
			transitionToFollower()
			pstore.PutCurrentTerm(reply.Term)
			return DemotedToFollower
		}
		return LogMismatch
	}
	vlstate.nextIndex[destID] += entryCount
	return nil
}

func broadcastHeartBeat() {
	errCh := broadcastToLogSenderDaemons(pstore.LogSize())
	for i := 0; i < len(grpcEndpoints)/2+1; i++ {
		err := <-errCh
		if err != nil {
			if errors.Is(err, DemotedToFollower) {
				break
			}
			slog.Error("Unexpected heartbeat error.",
				slog.String("err", err.Error()))
			os.Exit(1)
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
