package agent

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/peng225/starfish/internal/gmutex"
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
		slog.Error("Failed to listen.",
			slog.String("err", err.Error()))
		os.Exit(1)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	sfrpc.RegisterRaftServer(grpcServer, newRaftServer())
	err = grpcServer.Serve(lis)
	if err != nil {
		slog.Error("grpcServer.Serve failed.",
			slog.String("err", err.Error()))
		os.Exit(1)
	}
}

func (rsi *RaftServerImpl) AppendEntries(ctx context.Context, req *sfrpc.AppendEntriesRequest) (*sfrpc.AppendEntriesReply, error) {
	gmutex.Lock()
	defer gmutex.Unlock()
	electionTimeoutBase = time.Now()
	reply := &sfrpc.AppendEntriesReply{
		Term:    pstore.CurrentTerm(),
		Success: false,
	}

	switch {
	case req.Term < pstore.CurrentTerm():
		slog.Warn("req.Term is smaller than my term.",
			slog.Int("leaderID", int(req.LeaderID)),
			slog.Int64("requestTerm", req.Term),
			slog.Int64("term", pstore.CurrentTerm()))
		return reply, nil
	case req.Term == pstore.CurrentTerm():
		if vstate.role == Candidate {
			slog.Info("Got request with the same term when I am a candidate.",
				slog.Int("leaderID", int(req.LeaderID)),
				slog.Int64("term", pstore.CurrentTerm()))
			transitionToFollower()
		} else if vstate.role == Leader {
			slog.Error("Another leader found in the same term.",
				slog.Int("leaderID", int(req.LeaderID)))
			os.Exit(1)
		}
	case req.Term > pstore.CurrentTerm():
		slog.Info("req.Term is larger than my term.",
			slog.Int("leaderID", int(req.LeaderID)),
			slog.Int64("requestTerm", req.Term),
			slog.Int64("term", pstore.CurrentTerm()))
		transitionToFollower()
	}
	pstore.PutCurrentTerm(req.Term)
	vstate.currentLeaderID = req.LeaderID
	reply.Term = pstore.CurrentTerm()

	//Check the previous log entry match.
	if pstore.LogSize()-1 < req.PrevLogIndex {
		slog.Warn("The previous log does not match.",
			slog.Int64("requestPrevLogIndex", req.PrevLogIndex),
			slog.Int64("lastLogIndex", pstore.LogSize()-1))
		return reply, nil
	} else if req.PrevLogIndex >= 0 && pstore.LogEntry(req.PrevLogIndex).Term != req.PrevLogTerm {
		slog.Warn("The previous log does not match.",
			slog.Int64("requestPrevLogIndex", req.PrevLogIndex),
			slog.Int64("requestPrevLogTerm", req.PrevLogTerm),
			slog.Int64("prevLogTerm", pstore.LogEntry(req.PrevLogIndex).Term))
		return reply, nil
	}

	for i, entry := range req.Entries {
		entryIndex := req.PrevLogIndex + int64(i) + 1
		// If the corresponding entry exists but does not have the same term,
		// remove the mismatched entries.
		if entryIndex <= pstore.LogSize()-1 &&
			pstore.LogEntry(entryIndex).Term != entry.Term {
			slog.Warn("An entry with the same index found, but with the different term.",
				slog.Int64("entryIndex", entryIndex),
				slog.Int64("lastLogIndex", pstore.LogSize()-1),
				slog.Int64("entryTerm", pstore.LogEntry(entryIndex).Term),
				slog.Int64("requestEntryTerm", entry.Term))
			if entryIndex <= vstate.commitIndex {
				slog.Error("Commited log entries are going to be deleted.",
					slog.Int64("entryIndex", entryIndex),
					slog.Int64("commitIndex", vstate.commitIndex))
				os.Exit(1)
			}
			pstore.CutOffLogTail(entryIndex)
			break
		}
		// If the entry does not exist, it is appended.
		if pstore.LogSize() == entryIndex {
			pstore.AppendLog(&LogEntry{
				Term:         entry.Term,
				LockHolderID: entry.LockHolderID,
			})
		} else if pstore.LogSize() < entryIndex {
			slog.Error("Invalid entry index.",
				slog.Int64("logSize", pstore.LogSize()),
				slog.Int64("entryIndex", entryIndex))
			os.Exit(1)
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
	gmutex.Lock()
	defer gmutex.Unlock()
	reply := &sfrpc.RequestVoteReply{
		Term:        pstore.CurrentTerm(),
		VoteGranted: false,
	}
	switch {
	case req.Term < pstore.CurrentTerm():
		slog.Warn("req.Term is smaller than my term.",
			slog.Int64("requestTerm", req.Term),
			slog.Int64("term", pstore.CurrentTerm()))
		return reply, nil
	case req.Term > pstore.CurrentTerm():
		pstore.PutVotedFor(InvalidAgentID)
		transitionToFollower()
	default:
	}
	pstore.PutCurrentTerm(req.Term)
	reply.Term = pstore.CurrentTerm()

	if pstore.VotedFor() >= 0 && req.CandidateID != pstore.VotedFor() {
		slog.Warn("Already voted for someone else. Reject.",
			slog.Int("votedFor", int(pstore.VotedFor())))
		return reply, nil
	}
	llTerm := int64(-1)
	if pstore.LogSize() > 0 {
		llTerm = pstore.LogEntry(pstore.LogSize() - 1).Term
	}
	if req.LastLogTerm < llTerm ||
		(req.LastLogTerm == llTerm &&
			req.LastLogIndex < pstore.LogSize()-1) {
		slog.Warn("Candidate's last log is too old. Reject.",
			slog.Int64("requestLastLogTerm", req.LastLogTerm),
			slog.Int64("lastLogTerm", llTerm),
			slog.Int64("requestLastLogIndex", req.LastLogIndex),
			slog.Int64("lastLogIndex", pstore.LogSize()-1))
		return reply, nil
	}

	reply.VoteGranted = true
	pstore.PutVotedFor(req.CandidateID)
	return reply, nil
}
