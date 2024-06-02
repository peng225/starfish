package agent

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	sfrpc "github.com/peng225/starfish/internal/rpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RaftServerImpl struct {
	sfrpc.UnimplementedRaftServer
}

const (
	electionTimeoutSec = 2
)

var (
	lastReceived time.Time
)

func newRaftServer() *RaftServerImpl {
	return &RaftServerImpl{}
}

func (rsi *RaftServerImpl) AppendEntries(ctx context.Context, req *sfrpc.AppendEntriesRequest) (*sfrpc.AppendEntriesReply, error) {
	if vstate.role != Follower {
		return nil, status.Errorf(codes.Internal, fmt.Sprintf("invalid server state: %d", vstate.role))
	}
	lastReceived = time.Now()
	reply := &sfrpc.AppendEntriesReply{
		Term:    pstate.currentTerm,
		Success: false,
	}
	if req.Term < pstate.currentTerm {
		return reply, nil
	}
	// TODO: implement
	reply.Success = false
	return reply, nil
}
func (rsi *RaftServerImpl) RequestVote(ctx context.Context, req *sfrpc.RequestVoteRequest) (*sfrpc.RequestVoteReply, error) {
	if vstate.role != Follower {
		return nil, status.Errorf(codes.Internal, fmt.Sprintf("invalid server state: %d", vstate.role))
	}
	lastReceived = time.Now()
	reply := &sfrpc.RequestVoteReply{
		Term:        pstate.currentTerm,
		VoteGranted: false,
	}
	if req.Term < pstate.currentTerm {
		return reply, nil
	}
	if pstate.votedFor >= 0 && req.CandidateID != pstate.votedFor {
		return reply, nil
	}
	// TODO: implement
	reply.VoteGranted = false
	return reply, nil
}

// TODO: should stop when the process is not a follower.
func checkElectionTimeout() {
	lastReceived = time.Now()
	ticker := time.NewTicker(time.Microsecond * 100)
	r := rand.Intn(10)
	for {
		<-ticker.C
		if vstate.role != Follower {
			continue
		}
		if time.Since(lastReceived) > time.Second*time.Duration(electionTimeoutSec+r) {
			log.Println("Election timeout detected.")
			transitionToCandidate()
			Election()
			lastReceived = time.Now()
		}
	}
}

func StartFollower(port int) {
	// Start election timeout watcher.
	go checkElectionTimeout()

	// Start gRPC server.
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	sfrpc.RegisterRaftServer(grpcServer, newRaftServer())
	log.Fatal(grpcServer.Serve(lis))
}
