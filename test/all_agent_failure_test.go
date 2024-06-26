package test

import (
	"math/rand"
	"os/exec"
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAllAgentFailure(t *testing.T) {
	c := readConfig(t, "../config.yaml")

	lockHolder := 1
	lockRequest(t, lockHolder, c.WebServers[rand.Intn(len(c.WebServers))])
	checkLockHolder(t, lockHolder, c.WebServers[rand.Intn(len(c.WebServers))])

	pids := getAgentPIDs(t)

	var wg sync.WaitGroup
	wg.Add(len(pids))
	for _, pid := range pids {
		pid := pid
		go func() {
			sendSignal(t, syscall.SIGTERM, pid)
			wg.Done()
		}()
	}
	wg.Wait()

	cmd := exec.Command("make", "-C", "../", "run")
	err := cmd.Run()
	require.NoError(t, err)

	for _, server := range c.WebServers {
		t.Logf("server: %s", server)
		checkLockHolder(t, lockHolder, server)
	}

	unlockRequest(t, lockHolder, c.WebServers[rand.Intn(len(c.WebServers))])
}
