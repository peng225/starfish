package agent

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"time"

	"github.com/peng225/starfish/internal/gmutex"
	sfrpc "github.com/peng225/starfish/internal/rpc"
)

func electionWithRetry() {
	for {
		err := election()
		if err == nil {
			break
		}
	}
}

func election() error {
	gmutex.Lock()
	defer gmutex.Unlock()
	if vstate.role == Follower {
		slog.Info("Found that I have become a follower.")
		return nil
	}
	slog.Info("Election start.")
	electionTimeoutBase = time.Now()
	pstore.PutVotedFor(vstate.id)
	pstore.PutCurrentTerm(pstore.CurrentTerm() + 1)
	slog.Info("Updated the current term.",
		slog.Int64("term", pstore.CurrentTerm()))
	voteResult := make(chan bool, len(grpcServers))
	ctx, cancel := context.WithCancelCause(context.Background())
	for i := range grpcServers {
		i := i
		go func() {
			if i == int(vstate.id) {
				voteResult <- true
				return
			}
			llt := int64(-1)
			if pstore.LogSize() > 0 {
				llt = pstore.LogEntry(pstore.LogSize() - 1).Term
			}
			reply, err := rpcClients[i].RequestVote(ctx, &sfrpc.RequestVoteRequest{
				Term:         pstore.CurrentTerm(),
				CandidateID:  vstate.id,
				LastLogIndex: pstore.LogSize() - 1,
				LastLogTerm:  llt,
			})
			if err != nil {
				slog.Error("RequestVote RPC failed.",
					slog.String("dest", grpcServers[i]),
					slog.String("err", err.Error()))
				voteResult <- false
				return
			}
			if reply.Term > pstore.CurrentTerm() {
				slog.Info("Found larger term in the response of RequestVote RPC.",
					slog.String("dest", grpcServers[i]),
					slog.Int64("term", pstore.CurrentTerm()),
					slog.Int64("responseTerm", reply.Term))
				cancel(DemotedToFollower)
				transitionToFollower()
				pstore.PutCurrentTerm(reply.Term)
			}
			voteResult <- reply.VoteGranted
		}()
	}

	voteCount := 0
	successfulVoteCount := 0
	r := time.Duration(rand.Intn(electionTimeoutRandMaxMilliSec)) * time.Millisecond
	for {
		select {
		case <-time.After(electionTimeout + r):
			slog.Warn("Election timeout!")
			err := errors.New("election timeout")
			cancel(err)
			return err
		case res := <-voteResult:
			if res {
				successfulVoteCount++
				slog.Info("Got a vote.")
				if successfulVoteCount > len(grpcServers)/2 {
					err := transitionToLeader()
					if err != nil {
						slog.Error("Failed to promote to the leader.",
							slog.String("err", err.Error()))
						return err
					}
					return nil
				}
			}
			voteCount++
			if voteCount == len(grpcServers) {
				slog.Warn("Could not get enough votes.")
				time.Sleep(r / 2)
				return errors.New("not enough votes")
			}
		}
	}
}
