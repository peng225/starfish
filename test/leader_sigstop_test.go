package test

import (
	"bytes"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sendSignal(t *testing.T, sig syscall.Signal, pid int) {
	err := syscall.Kill(pid, sig)
	require.NoError(t, err)
}

func TestLeaderSigStop(t *testing.T) {
	c := readConfig(t, "../config.yaml")

	lockHolder := strconv.Itoa(1)
	require.Eventually(t, func() bool {
		req, err := http.NewRequest(http.MethodPut,
			c.WebEndpoints[rand.Intn(len(c.WebEndpoints))]+"/lock",
			bytes.NewBuffer([]byte(lockHolder)))
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		return resp.StatusCode == http.StatusOK
	}, 2*time.Second, 10*time.Microsecond)

	require.Eventually(t, func() bool {
		resp, err := http.Get(c.WebEndpoints[rand.Intn(len(c.WebEndpoints))] + "/lock")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		data, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, lockHolder, string(data))
		return true
	}, 2*time.Second, 10*time.Microsecond)

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

	for _, pid := range pids {
		sendSignal(t, syscall.SIGSTOP, pid)
		time.Sleep(10 * time.Second)
		sendSignal(t, syscall.SIGCONT, pid)
		for _, endpoint := range c.WebEndpoints {
			require.Eventually(t, func() bool {
				t.Logf("Stopped PID: %d", pid)
				t.Logf("endpoint: %s", endpoint)
				resp, err := http.Get(endpoint + "/lock")
				require.NoError(t, err)
				defer resp.Body.Close()
				t.Logf("statusCode: %d", resp.StatusCode)
				require.Equal(t, http.StatusOK, resp.StatusCode)
				data, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				t.Logf("lockHolder: %s", string(data))
				require.Equal(t, lockHolder, string(data))
				return true
			}, 2*time.Second, 20*time.Microsecond)
		}
	}
}
