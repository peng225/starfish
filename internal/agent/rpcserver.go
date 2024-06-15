package agent

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	sfrpc "github.com/peng225/starfish/internal/rpc"
	"google.golang.org/grpc"
)

type RaftServerImpl struct {
	sfrpc.UnimplementedRaftServer
}

const (
	electionTimeout = 2 * time.Second
)

var (
	electionTimeoutBase time.Time
)

func newRaftServer() *RaftServerImpl {
	return &RaftServerImpl{}
}

func StartRPCServer(port int) {
	// Start gRPC server.
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	sfrpc.RegisterRaftServer(grpcServer, newRaftServer())
	log.Fatal(grpcServer.Serve(lis))
}

func (rsi *RaftServerImpl) AppendEntries(ctx context.Context, req *sfrpc.AppendEntriesRequest) (*sfrpc.AppendEntriesReply, error) {
	electionTimeoutBase = time.Now()
	reply := &sfrpc.AppendEntriesReply{
		Term:    pstore.CurrentTerm(),
		Success: false,
	}

	switch {
	case req.Term < pstore.CurrentTerm():
		return reply, nil
	case req.Term == pstore.CurrentTerm():
		if vstate.role == Candidate {
			transitionToFollower()
		} else if vstate.role == Leader {
			log.Fatalf("Another leader found in the same term. ID: %d", req.LeaderID)
		}
	case req.Term > pstore.CurrentTerm():
		transitionToFollower()
	}
	pstore.PutCurrentTerm(req.Term)
	vstate.currentLeaderID = req.LeaderID
	reply.Term = pstore.CurrentTerm()

	//Check the previous log entry match.
	if pstore.LogSize()-1 < req.PrevLogIndex ||
		(req.PrevLogIndex > 0 && pstore.LogEntry(req.PrevLogIndex).Term != req.PrevLogTerm) {
		return reply, nil
	}

	for i, entry := range req.Entries {
		entryIndex := req.PrevLogIndex + int64(i) + 1
		// If the corresponding entry exists but does not have the same term,
		// remove the mismatched entries.
		if entryIndex <= pstore.LogSize()-1 &&
			pstore.LogEntry(entryIndex).Term != req.Term {
			pstore.CutOffLogTail(entryIndex)
			break
		}
		// If the entry does not exist, it is appended.
		if pstore.LogSize() == entryIndex {
			pstore.AppendLog(&LogEntry{
				Term:         req.Term,
				LockHolderID: entry.LockHolderID,
			})
		} else if pstore.LogSize() < entryIndex {
			log.Fatalf("Invalid entry index. pstore.LogSize(): %d, entryIndex: %d",
				pstore.LogSize(), entryIndex)
		}
		// When you reach here, the log has already been appended,
		// and no operation is executed to make the gRPC call idempotent.
	}

	if req.LeaderCommit > vstate.commitIndex {
		updateCommitIndex(min(req.LeaderCommit, pstore.LogSize()-1))
	}

	reply.Success = true
	return reply, nil
}
func (rsi *RaftServerImpl) RequestVote(ctx context.Context, req *sfrpc.RequestVoteRequest) (*sfrpc.RequestVoteReply, error) {
	reply := &sfrpc.RequestVoteReply{
		Term:        pstore.CurrentTerm(),
		VoteGranted: false,
	}
	switch {
	case req.Term < pstore.CurrentTerm():
		return reply, nil
	case req.Term > pstore.CurrentTerm():
		pstore.PutVotedFor(InvalidAgentID)
		transitionToFollower()
	default:
	}
	pstore.PutCurrentTerm(req.Term)
	reply.Term = pstore.CurrentTerm()

	if pstore.VotedFor() >= 0 && req.CandidateID != pstore.VotedFor() {
		return reply, nil
	}
	llt := int64(-1)
	if pstore.LogSize() > 0 {
		llt = pstore.LogEntry(pstore.LogSize() - 1).Term
	}
	if req.LastLogTerm < llt ||
		(req.LastLogTerm == llt &&
			req.LastLogIndex < pstore.LogSize()-1) {
		return reply, nil
	}

	reply.VoteGranted = true
	pstore.PutVotedFor(req.CandidateID)
	return reply, nil
}
