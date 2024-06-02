package agent

import (
	"context"
	"log"
	"sync"
	"time"

	sfrpc "github.com/peng225/starfish/internal/rpc"
	"google.golang.org/grpc"
)

func Election() {
	pstate.votedFor = vstate.id
	// TODO: save votedFor to drive.
	voteResult := make(map[int32]bool)
	voteResult[vstate.id] = true
	var wg sync.WaitGroup
	wg.Add(len(addrs))
	for i, addr := range addrs {
		i := i
		addr := addr
		go func() {
			defer wg.Done()
			if i == int(vstate.id) {
				return
			}
			// Maybe I have to retry until the election timeout.
			for i := 0; i < 3; i++ {
				var opts []grpc.DialOption
				conn, err := grpc.NewClient(addr, opts...)
				if err != nil {
					log.Printf("Failed to connect to %s. err: %s", addr, err.Error())
					continue
				}
				defer conn.Close()

				client := sfrpc.NewRaftClient(conn)

				reply, err := client.RequestVote(context.Background(), &sfrpc.RequestVoteRequest{
					Term:         pstate.currentTerm,
					CandidateID:  vstate.id,
					LastLogIndex: 0,
					LastLogTerm:  0,
				})
				if err != nil {
					log.Printf("AppendEntries RPC for %s failed. err: %s", addr, err.Error())
					continue
				}
				if reply.VoteGranted {
					voteResult[int32(i)] = true
					return
				}
				time.Sleep(time.Microsecond * 100)
			}
		}()
	}
	wg.Wait()

	voteCount := 0
	for _, v := range voteResult {
		if v {
			voteCount++
		}
	}
	if voteCount > len(addrs)/2 {
		TransitionToLeader()
	} else {
		TransitionToFollower()
	}
}
