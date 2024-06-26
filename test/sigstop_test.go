package test

import (
	"log"
	"math/rand"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func sendSignal(t *testing.T, sig syscall.Signal, pid int) {
	t.Helper()
	err := syscall.Kill(pid, sig)
	require.NoError(t, err)
}

func getAgentPIDs(t *testing.T) []int {
	t.Helper()
	cmd := exec.Command("pidof", "starfish")
	var out strings.Builder
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		log.Fatal(err)
	}

	pidsStr := strings.Split(out.String(), " ")
	pids := make([]int, 0, len(pidsStr))
	for _, pidStr := range pidsStr {
		pid, err := strconv.Atoi(strings.TrimSpace(pidStr))
		require.NoError(t, err)
		pids = append(pids, pid)
	}
	return pids
}

func TestSigStop(t *testing.T) {
	c := readConfig(t, "../config.yaml")

	lockHolder := 1
	lockRequest(t, lockHolder, c.WebServers[rand.Intn(len(c.WebServers))])
	checkLockHolder(t, lockHolder, c.WebServers[rand.Intn(len(c.WebServers))])

	pids := getAgentPIDs(t)

	for _, pid := range pids {
		sendSignal(t, syscall.SIGSTOP, pid)
		time.Sleep(10 * time.Second)
		sendSignal(t, syscall.SIGCONT, pid)
		for _, server := range c.WebServers {
			t.Logf("Stopped PID: %d", pid)
			t.Logf("server: %s", server)
			checkLockHolder(t, lockHolder, server)
		}
	}

	unlockRequest(t, lockHolder, c.WebServers[rand.Intn(len(c.WebServers))])
}
