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
		pstate.votedFor = vstate.id
		pstate.currentTerm++
		// TODO: save votedFor to drive.
		voteResult := make(chan bool, len(addrs))
		ctx, cancel := context.WithCancelCause(context.Background())
		for i := range addrs {
			i := i
			go func() {
				if i == int(vstate.id) {
					voteResult <- true
					return
				}
				reply, err := rpcClients[i].RequestVote(ctx, &sfrpc.RequestVoteRequest{
					Term:         pstate.currentTerm,
					CandidateID:  vstate.id,
					LastLogIndex: 0,
					LastLogTerm:  0,
				})
				if err != nil {
					log.Printf("RequestVote RPC for %s failed. err: %s", addrs[i], err.Error())
					voteResult <- false
					return
				}
				if reply.Term > pstate.currentTerm {
					log.Printf("Found larger term in the response of RequestVote RPC for %s. term: %d, response term: %d",
						addrs[i], pstate.currentTerm, reply.Term)
					cancel(DemotedToFollower)
					transitionToFollower(reply.Term)
				}
				voteResult <- reply.VoteGranted
			}()
		}

		voteCount := 0
		r := time.Duration(rand.Intn(10)) * time.Second
	WaitForVote:
		for {
			select {
			case <-time.After(electionTimeoutSec + r):
				log.Println("Election timeout!")
				cancel(errors.New("election timeout"))
				break WaitForVote
			case res := <-voteResult:
				if res {
					voteCount++
					log.Println("Got a vote.")
					if voteCount > len(addrs)/2 {
						err := transitionToLeader()
						if err != nil {
							log.Printf("Failed to promote to the leader. err: %s", err)
							break WaitForVote
						}
						return
					}
				}
			}
		}
	}
}
