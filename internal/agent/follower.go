package agent

import (
	"log/slog"
	"math/rand"
	"time"
)

func checkElectionTimeout() {
	electionTimeoutBase = time.Now()
	ticker := time.NewTicker(time.Microsecond * 100)
	r := time.Duration(rand.Intn(10)) * time.Second
	for {
		<-ticker.C
		if time.Since(electionTimeoutBase) > electionTimeout+r {
			slog.Info("Election timeout detected.")
			transitionToCandidate()
			go election()
			break
		}
	}
}
