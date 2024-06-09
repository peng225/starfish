package agent

import (
	"log"
	"math/rand"
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
}

type PersistentState struct {
	currentTerm int64
	votedFor    int32
	log         []LogEntry
}

type VolatileState struct {
	id          int32
	role        Role
	commitIndex int64
	lastApplied int64
}

var (
	sm     StateMachine
	pstate PersistentState
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
	pstate = PersistentState{
		currentTerm: 0,
		votedFor:    InvalidAgentID,
		log:         make([]LogEntry, 0),
	}
	vstate = VolatileState{
		id:          0,
		role:        Follower,
		commitIndex: -1,
		lastApplied: -1,
	}
	notifyLogApply = make(chan struct{}, 1)
}

func Init(id int32, ge []string) {
	vstate.id = id
	grpcEndpoints = ge

	// gRPC client setup.
	for _, addr := range grpcEndpoints {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Failed to connect to %s. err: %s", addr, err.Error())
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
	log.Printf("Transition to leader. term: %d", pstate.currentTerm)
	initLeaderOnPromotion()
	vstate.role = Leader
	go heartBeatDaemon()
	return nil
}

func transitionToFollower() {
	muStateTransition.Lock()
	defer muStateTransition.Unlock()
	if vstate.role == Follower {
		return
	}
	log.Println("Transition to follower.")
	vstate.role = Follower
	go checkElectionTimeout()
}

func transitionToCandidate() {
	muStateTransition.Lock()
	defer muStateTransition.Unlock()
	if vstate.role == Candidate {
		return
	}
	log.Println("Transition to candidate.")
	vstate.role = Candidate
}

func IsLeader() bool {
	return vstate.role == Leader
}

func LeaderID() int32 {
	if vstate.role == Candidate {
		return InvalidAgentID
	}
	if pstate.votedFor == InvalidAgentID {
		return rand.Int31n(int32(len(grpcEndpoints)))
	}
	return pstate.votedFor
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
	log := pstate.log[logIndex]
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
			log.Fatalf("invalid commit and last applied index. commitIndex: %d, lastApplied: %d",
				vstate.commitIndex, vstate.lastApplied)
		}
	}
}
