package agent

import (
	"log"
	"math/rand"
	"time"
)

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

func StartFollower() {
	// Start election timeout watcher.
	go checkElectionTimeout()
}
