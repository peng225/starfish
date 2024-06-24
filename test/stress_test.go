package test

import (
	"math/rand"
	"sync"
	"testing"
)

func TestStress(t *testing.T) {
	c := readConfig(t, "../config.yaml")

	wg := sync.WaitGroup{}
	clientCount := 16
	wg.Add(clientCount)
	for i := 0; i < clientCount; i++ {
		go func(clientID int) {
			defer wg.Done()
			// Lock, check and unlock.
			lockRequest(t, clientID, c.WebServers[rand.Intn(len(c.WebServers))])
			checkLockHolder(t, clientID, c.WebServers[rand.Intn(len(c.WebServers))])
			unlockRequest(t, clientID, c.WebServers[rand.Intn(len(c.WebServers))])
		}(i)
	}
	wg.Wait()
}
