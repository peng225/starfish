package agent

import (
	"log/slog"
	"os"
	"sync"

	sfrpc "github.com/peng225/starfish/internal/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Role int32

const (
	Follower Role = iota
	Candidate
	Leader

	InvalidLockHolderID int32 = -1
	InvalidAgentID      int32 = -1
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

	muStateTransition sync.Mutex

	notifyLogApply chan struct{}
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
	muStateTransition.Lock()
	defer muStateTransition.Unlock()
	if vstate.role == Leader {
		return nil
	}
	slog.Info("Transition to leader.",
		slog.Int64("term", pstore.CurrentTerm()))
	initLeaderOnPromotion()
	vstate.role = Leader
	vstate.currentLeaderID = vstate.id
	go heartBeatDaemon()
	return nil
}

func transitionToFollower() {
	muStateTransition.Lock()
	defer muStateTransition.Unlock()
	if vstate.role == Follower {
		return
	}
	slog.Info("Transition to follower.")
	vstate.role = Follower
	go checkElectionTimeout()
}

func transitionToCandidate() {
	muStateTransition.Lock()
	defer muStateTransition.Unlock()
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
	log := pstore.LogEntry(logIndex)
	sm.LockHolderID = log.LockHolderID
}

func applierDaemon() {
	for {
		<-notifyLogApply
		if vstate.lastApplied < vstate.commitIndex {
			for logIndex := vstate.lastApplied + 1; logIndex <= vstate.commitIndex; logIndex++ {
				applyLog(logIndex)
				vstate.lastApplied++
			}
		} else if vstate.commitIndex < vstate.lastApplied {
			slog.Error("Invalid commit and last applied index.",
				slog.Int64("commitIndex", vstate.commitIndex),
				slog.Int64("lastApplied", vstate.lastApplied))
			os.Exit(1)
		}
	}
}
