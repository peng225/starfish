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

var (
	vlstate           VolatileLeaderState
	DemotedToFollower error
)

func init() {
	vlstate = VolatileLeaderState{
		// FIXME: The number of agents is fixed to 3.
		nextIndex:  make([]int64, 3),
		matchIndex: make([]int64, 3),
	}
	DemotedToFollower = errors.New("demoted to the follower")
}

func AppendLog(logEntry *LogEntry) error {
	logEntry.Term = pstate.currentTerm
	pstate.log = append(pstate.log, *logEntry)
	// TODO: save to disk

	errCh := sendLogToAllWithRetry(logEntry)
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

func sendLogToAllWithRetry(logEntry *LogEntry) chan error {
	errCh := make(chan error, len(addrs)-1)
	for i := range addrs {
		i := i
		ctx, cancel := context.WithCancelCause(context.Background())
		go func() {
			for {
				if i == int(vstate.id) {
					return
				}
				err := sendLog(ctx, int32(i), []LogEntry{*logEntry})
				if errors.Is(err, DemotedToFollower) {
					log.Println("Demoted to the follower.")
					cancel(DemotedToFollower)
					errCh <- DemotedToFollower
					break
				} else if err != nil {
					time.Sleep(time.Millisecond * 100)
					continue
				}
				errCh <- nil
				break
			}
		}()
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
