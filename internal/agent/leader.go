package agent

import (
	"context"
	"log"
	"time"

	sfrpc "github.com/peng225/starfish/internal/rpc"

	"google.golang.org/grpc"
)

type VolatileLeaderState struct {
	nextIndex  []int64
	matchIndex []int64
}

var (
	vlstate VolatileLeaderState
)

func init() {
	vlstate = VolatileLeaderState{
		// FIXME: The number of agents is fixed to 3.
		nextIndex:  make([]int64, 3),
		matchIndex: make([]int64, 3),
	}
}

func AppendLog(logEntries []LogEntry) {
	for i := 0; i < len(logEntries); i++ {
		logEntries[i].Term = pstate.currentTerm
	}
	pstate.log = append(pstate.log, logEntries...)
	// TODO: save to disk

	sendLog(logEntries)
}

func sendLog(logEntries []LogEntry) {
	for i, addr := range addrs {
		if i == int(vstate.id) {
			continue
		}

		var opts []grpc.DialOption
		conn, err := grpc.NewClient(addr, opts...)
		if err != nil {
			log.Printf("Failed to connect to %s. err: %s", addr, err.Error())
			continue
		}
		defer conn.Close()

		client := sfrpc.NewRaftClient(conn)
		entries := make([]*sfrpc.LogEntry, 0)
		for _, e := range logEntries {
			entries = append(entries, &sfrpc.LogEntry{
				LockHolderID: e.LockHolderID,
			})
		}
		reply, err := client.AppendEntries(context.Background(), &sfrpc.AppendEntriesRequest{
			Term:         pstate.currentTerm,
			LeaderID:     vstate.id,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      entries,
			LeaderCommit: vstate.commitIndex,
		})
		if err != nil {
			log.Printf("AppendEntries RPC for %s failed. err: %s", addr, err.Error())
			continue
		}
		log.Println(reply)
	}
}

func sendHeartBeat() {
	ticker := time.NewTicker(time.Second)
	for {
		<-ticker.C
		// TODO: we may need a lock to check the current role.
		if vstate.role == Leader {
			sendLog(nil)
		}
	}
}
