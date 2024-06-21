package agent

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/peng225/deduplog"
	sfrpc "github.com/peng225/starfish/internal/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Role int32

const (
	Follower Role = iota
	Candidate
	Leader

	InvalidLockHolderID       int32 = -1
	InvalidAgentID            int32 = -1
	NoopAgentID               int32 = -2
	electionTimeout                 = 2 * time.Second
	electionTimeoutRandMaxSec       = 5
)

type StateMachine struct {
	LockHolderID int32
}

type LogEntry struct {
	Term         int64
	LockHolderID int32
	Reserved     int32
}

type VolatileState struct {
	id              int32
	role            Role
	commitIndex     int64
	lastApplied     int64
	currentLeaderID int32
}

var (
	sm     StateMachine
	pstore PersistentStore
	vstate VolatileState

	grpcEndpoints []string
	rpcClients    []sfrpc.RaftClient

	notifyLogApply chan struct{}
	dedupLogger    *slog.Logger
)

func init() {
	sm = StateMachine{
		LockHolderID: InvalidLockHolderID,
	}
	vstate = VolatileState{
		id:              0,
		role:            Follower,
		commitIndex:     -1,
		lastApplied:     -1,
		currentLeaderID: InvalidAgentID,
	}
	notifyLogApply = make(chan struct{}, 1)
}

func Init(id int32, ge []string, ps PersistentStore) {
	vstate.id = id
	grpcEndpoints = ge

	pstore = ps

	// gRPC client setup.
	for _, addr := range grpcEndpoints {
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			slog.Error("grpc.NewClient failed.",
				slog.String("addr", addr),
				slog.String("err", err.Error()))
			return
		}

		rpcClients = append(rpcClients, sfrpc.NewRaftClient(conn))
	}

	dedupLogger = slog.New(deduplog.NewDedupHandler(context.Background(),
		slog.Default().Handler(),
		&deduplog.HandlerOptions{
			HistoryRetentionPeriod: 2 * time.Second,
			MaxHistoryCount:        deduplog.DefaultMaxHistoryCount,
		}))

	initLeader()

	// Start daemons.
	for i := range grpcEndpoints {
		i := i
		if int32(i) == vstate.id {
			continue
		}
		go logSenderDaemon(int32(i))
	}
	go checkElectionTimeout()
	go applierDaemon()
}

func transitionToLeader() error {
	if vstate.role == Leader {
		return nil
	}

	slog.Info("Try to transition to leader.",
		slog.Int64("term", pstore.CurrentTerm()))
	// Initialize leader-related variables.
	// This is required to send a no-op entry.
	initLeaderOnPromotion()
	// Here, we want to commit the entries in the past terms.
	// However, the Rust algorithm allows the leader
	// only to commit entries of its own term.
	// Thus, we have to put a no-op entry and commit it,
	// which leads to committing all the older entries.
	e := &LogEntry{
		Term:         pstore.CurrentTerm(),
		LockHolderID: NoopAgentID,
	}
	err := AppendLog(e)
	if err != nil {
		return err
	}

	vstate.role = Leader
	vstate.currentLeaderID = vstate.id
	go heartBeatDaemon()
	slog.Info("Leader transition completed.",
		slog.Int64("term", pstore.CurrentTerm()))
	return nil
}

func transitionToFollower() {
	if vstate.role == Follower {
		return
	}
	slog.Info("Transition to follower.")
	vstate.role = Follower
	for _, queue := range sendLogQueues {
		for len(queue) != 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}
	go checkElectionTimeout()
}

func transitionToCandidate() {
	if vstate.role == Candidate {
		return
	}
	slog.Info("Transition to candidate.")
	vstate.role = Candidate
	vstate.currentLeaderID = InvalidAgentID
}

func IsLeader() bool {
	return vstate.role == Leader
}

func LeaderID() int32 {
	return vstate.currentLeaderID
}

func LockHolderID() int32 {
	return sm.LockHolderID
}

func PendingApplyLogExist() bool {
	return vstate.lastApplied < vstate.commitIndex
}

func updateCommitIndex(ci int64) {
	vstate.commitIndex = ci
	if len(notifyLogApply) == 0 {
		notifyLogApply <- struct{}{}
	}
}

func applyLog(logIndex int64) {
	defer func() { vstate.lastApplied++ }()
	log := pstore.LogEntry(logIndex)
	if log == nil {
		slog.Error("Invalid log index.", slog.Int64("logIndex", logIndex),
			slog.Int64("logSisze", pstore.LogSize()))
		os.Exit(1)
	}
	if log.LockHolderID == NoopAgentID {
		return
	}
	sm.LockHolderID = log.LockHolderID
}

func applierDaemon() {
	for {
		<-notifyLogApply
		if vstate.lastApplied < vstate.commitIndex {
			for logIndex := vstate.lastApplied + 1; logIndex <= vstate.commitIndex; logIndex++ {
				applyLog(logIndex)
			}
		} else if vstate.commitIndex < vstate.lastApplied {
			slog.Error("Invalid commit and last applied index.",
				slog.Int64("commitIndex", vstate.commitIndex),
				slog.Int64("lastApplied", vstate.lastApplied))
			os.Exit(1)
		}
	}
}
