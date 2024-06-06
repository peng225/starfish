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
	electionTimeoutSec = 2 * time.Second
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
		Term:    pstate.currentTerm,
		Success: false,
	}
	switch {
	case req.Term < pstate.currentTerm:
		return reply, nil
	case req.Term == pstate.currentTerm:
		if vstate.role == Candidate {
			transitionToFollower(req.Term)
		}
	case req.Term > pstate.currentTerm:
		transitionToFollower(req.Term)
	}
	// TODO: implement
	reply.Success = true
	return reply, nil
}
func (rsi *RaftServerImpl) RequestVote(ctx context.Context, req *sfrpc.RequestVoteRequest) (*sfrpc.RequestVoteReply, error) {
	reply := &sfrpc.RequestVoteReply{
		Term:        pstate.currentTerm,
		VoteGranted: false,
	}
	switch {
	case req.Term < pstate.currentTerm:
		return reply, nil
	case req.Term > pstate.currentTerm:
		transitionToFollower(req.Term)
	default:
	}
	if pstate.votedFor >= 0 && req.CandidateID != pstate.votedFor {
		return reply, nil
	}
	// TODO: implement
	reply.VoteGranted = true
	pstate.votedFor = req.CandidateID
	// TODO: save to disk
	return reply, nil
}
