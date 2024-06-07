package agent

import (
	"log"
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
	InvalidAgentID            = -1
)

type MainState struct {
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
	mstate MainState
	pstate PersistentState
	vstate VolatileState

	addrs      []string
	rpcClients []sfrpc.RaftClient

	muStateTransition sync.Mutex
)

func init() {
	mstate = MainState{
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

	addrs = []string{
		"localhost:8080",
		"localhost:8081",
		"localhost:8082",
	}

	for _, addr := range addrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Failed to connect to %s. err: %s", addr, err.Error())
			return
		}

		rpcClients = append(rpcClients, sfrpc.NewRaftClient(conn))
	}
}

func Init(id int32) {
	vstate.id = id
}

func transitionToLeader() error {
	muStateTransition.Lock()
	defer muStateTransition.Unlock()
	if vstate.role == Leader {
		return nil
	}
	log.Println("Transition to leader.")
	err := sendHeartBeat()
	if err != nil {
		return err
	}
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

func LeaderAddr() string {
	if vstate.role == Candidate {
		return ""
	}
	return addrs[pstate.votedFor]
}

func LockHolderID() int32 {
	return mstate.LockHolderID
}

func StartDaemons() {
	for i := range addrs {
		i := i
		go sendLogDaemon(int32(i))
	}
	go checkElectionTimeout()
}
