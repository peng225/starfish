package agent

import (
	"log"
	"math/rand"
	"time"
)

func checkElectionTimeout() {
	electionTimeoutBase = time.Now()
	ticker := time.NewTicker(time.Microsecond * 100)
	r := time.Duration(rand.Intn(10)) * time.Second
	for {
		<-ticker.C
		if time.Since(electionTimeoutBase) > electionTimeoutSec+r {
			log.Println("Election timeout detected.")
			transitionToCandidate()
			go election()
			break
		}
	}
}
