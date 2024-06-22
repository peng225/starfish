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
	lockRequest(t, lockHolder, c.WebEndpoints[rand.Intn(len(c.WebEndpoints))])
	checkLockHolder(t, lockHolder, c.WebEndpoints[rand.Intn(len(c.WebEndpoints))])

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

	for _, endpoint := range c.WebEndpoints {
		t.Logf("endpoint: %s", endpoint)
		checkLockHolder(t, lockHolder, endpoint)
	}

	unlockRequest(t, lockHolder, c.WebEndpoints[rand.Intn(len(c.WebEndpoints))])
}
