package agent

import (
	"log/slog"
	"math/rand"
	"time"
)

func checkElectionTimeout() {
	electionTimeoutBase = time.Now()
	ticker := time.NewTicker(time.Microsecond * 100)
	r := time.Duration(rand.Intn(electionTimeoutRandMaxMilliSec)) * time.Millisecond
	for {
		<-ticker.C
		if time.Since(electionTimeoutBase) > electionTimeout+r {
			slog.Warn("Election timeout detected.")
			transitionToCandidate()
			go electionWithRetry()
			break
		}
	}
}
