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
	queueLength     = 100000
	heartBeatPeriod = 500 * time.Millisecond
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
		nextIndex:  make([]int64, len(grpcServers)),
		matchIndex: make([]int64, len(grpcServers)),
	}

	sendLogQueues = make([]chan sendLogRequest, len(grpcServers))
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
	slog.Debug("AppendLog start.")
	defer slog.Debug("AppendLog end.")
	logEntry.Term = pstore.CurrentTerm()
	pstore.AppendLog(logEntry)

	errCh := broadcastToLogSenderDaemons(pstore.LogSize())
	for i := 0; i < len(grpcServers)/2; i++ {
		err := <-errCh
		if err != nil {
			return err
		}
	}

	updateCommitIndex(pstore.LogSize() - 1)
	return nil
}

func broadcastToLogSenderDaemons(endIndex int64) chan error {
	errCh := make(chan error, len(grpcServers)-1)
	ctx, cancel := context.WithCancelCause(context.Background())
	for i := range grpcServers {
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
		if vstate.role == Follower {
			slog.Info("Found that I have become a follower.")
			cancel(DemotedToFollower)
			errCh <- DemotedToFollower
			return DemotedToFollower
		}
		if endIndex < vlstate.nextIndex[destID] {
			slog.Warn("nextIndex found to be larger than endIndex. Maybe demoted to a follower.",
				slog.Int("dest", int(destID)),
				slog.Int64("endIndex", endIndex),
				slog.Int64("nextIndex", vlstate.nextIndex[destID]))
			cancel(DemotedToFollower)
			errCh <- DemotedToFollower
			return DemotedToFollower
		}
		err := sendLog(ctx, destID, endIndex-vlstate.nextIndex[destID])
		if err != nil {
			if errors.Is(err, DemotedToFollower) {
				slog.Info("Demoted to the follower.")
				cancel(DemotedToFollower)
				errCh <- err
				return err
			} else if errors.Is(err, LogMismatch) {
				slog.Info("Log mismatch found.",
					slog.Int("dest", int(destID)),
					slog.Int64("nextIndex", vlstate.nextIndex[destID]))
				vlstate.nextIndex[destID]--
				if vlstate.nextIndex[destID] < 0 {
					slog.Error("nextIndex must not be a negative number.",
						slog.Int("dest", int(destID)),
						slog.Int64("nextIndex", vlstate.nextIndex[destID]))
					os.Exit(1)
				}
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
		e := pstore.LogEntry(i)
		if e == nil {
			slog.Error("Invalid log entry.",
				slog.Int("dest", int(destID)),
				slog.Int64("entryCount", entryCount),
				slog.Int64("index", i),
				slog.Int64("logSize", pstore.LogSize()))
			os.Exit(1)
		}
		entries = append(entries, &sfrpc.LogEntry{
			Term:         e.Term,
			LockHolderID: e.LockHolderID,
		})
	}
	plt := int64(-1)
	if vlstate.nextIndex[destID] > 0 {
		e := pstore.LogEntry(vlstate.nextIndex[destID] - 1)
		if e == nil {
			slog.Error("Invalid log entry.",
				slog.Int("dest", int(destID)),
				slog.Int64("entryCount", entryCount),
				slog.Int64("index", vlstate.nextIndex[destID]-1),
				slog.Int64("logSize", pstore.LogSize()))
			os.Exit(1)
		}
		plt = e.Term
	}
	cTerm := pstore.CurrentTerm()
	reply, err := rpcClients[destID].AppendEntries(ctx, &sfrpc.AppendEntriesRequest{
		Term:         cTerm,
		LeaderID:     vstate.id,
		PrevLogIndex: vlstate.nextIndex[destID] - 1,
		PrevLogTerm:  plt,
		Entries:      entries,
		LeaderCommit: vstate.commitIndex,
	})
	if err != nil {
		dedupLogger.Error("AppendEntries RPC failed.",
			slog.String("dest", grpcServers[destID]),
			slog.String("err", err.Error()))
		return err
	}
	if !reply.Success {
		dedupLogger.Error("AppendEntries RPC failed.",
			slog.String("dest", grpcServers[destID]),
			slog.Int64("replyTerm", reply.Term),
			slog.Int64("term", cTerm))
		if reply.Term > cTerm {
			return DemotedToFollower
		}
		return LogMismatch
	}
	vlstate.nextIndex[destID] += entryCount
	return nil
}

func BroadcastHeartBeat() error {
	errCh := broadcastToLogSenderDaemons(pstore.LogSize())
	for i := 0; i < len(grpcServers)/2; i++ {
		err := <-errCh
		if err != nil {
			return err
		}
	}
	return nil
}

func heartBeatDaemon() {
	ticker := time.NewTicker(heartBeatPeriod)
	for {
		<-ticker.C
		if vstate.role != Leader {
			break
		}
		err := BroadcastHeartBeat()
		if err != nil {
			slog.Error("Heartbeat failed.",
				slog.String("err", err.Error()))
		}
	}
}
