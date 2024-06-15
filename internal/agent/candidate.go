package agent

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"time"

	sfrpc "github.com/peng225/starfish/internal/rpc"
)

func election() {
	for {
		if vstate.role == Follower {
			log.Println("Found that I have become a follower.")
			return
		}
		log.Println("Election start.")
		electionTimeoutBase = time.Now()
		pstore.PutVotedFor(vstate.id)
		pstore.PutCurrentTerm(pstore.CurrentTerm() + 1)
		// TODO: save votedFor to drive.
		voteResult := make(chan bool, len(grpcEndpoints))
		ctx, cancel := context.WithCancelCause(context.Background())
		for i := range grpcEndpoints {
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
					log.Printf("RequestVote RPC for %s failed. err: %s", grpcEndpoints[i], err.Error())
					voteResult <- false
					return
				}
				if reply.Term > pstore.CurrentTerm() {
					log.Printf("Found larger term in the response of RequestVote RPC for %s. term: %d, response term: %d",
						grpcEndpoints[i], pstore.CurrentTerm(), reply.Term)
					cancel(DemotedToFollower)
					transitionToFollower()
					pstore.PutCurrentTerm(reply.Term)
				}
				voteResult <- reply.VoteGranted
			}()
		}

		voteCount := 0
		r := time.Duration(rand.Intn(10)) * time.Second
	WaitForVote:
		for {
			select {
			case <-time.After(electionTimeout + r):
				log.Println("Election timeout!")
				cancel(errors.New("election timeout"))
				break WaitForVote
			case res := <-voteResult:
				if res {
					voteCount++
					log.Println("Got a vote.")
					if voteCount > len(grpcEndpoints)/2 {
						err := transitionToLeader()
						if err != nil {
							// TODO: What is the expectation?
							// log.Printf("Failed to promote to the leader. err: %s", err)
							// break WaitForVote
						}
						return
					}
				}
			}
		}
	}
}
