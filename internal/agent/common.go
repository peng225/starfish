package agent

import "log"

type Role int32

const (
	Follower Role = iota
	Candidate
	Leader

	InvalidLockHolderID int32 = -1
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

	addrs []string
)

func init() {
	mstate = MainState{
		LockHolderID: -1,
	}
	pstate = PersistentState{
		currentTerm: 0,
		votedFor:    0,
		log:         make([]LogEntry, 0),
	}
	vstate = VolatileState{
		id:          0,
		role:        Follower,
		commitIndex: 0,
		lastApplied: 0,
	}

	addrs = []string{
		"localhost:8080",
		"localhost:8081",
		"localhost:8082",
	}
}

func transitionToLeader() {
	log.Println("Transition to leader.")
	vstate.role = Leader
}

func transitionToFollower() {
	log.Println("Transition to follower.")
	vstate.role = Follower
}

func transitionToCandidate() {
	log.Println("Transition to candidate.")
	vstate.role = Candidate
}

func IsLeader() bool {
	go sendHeartBeat()
	return vstate.role == Leader
}

func LeaderAddr() string {
	return addrs[pstate.votedFor]
}

func LockHolderID() int32 {
	return mstate.LockHolderID
}
