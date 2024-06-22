package test

import (
	"bytes"
	"io"
	"math/rand"
	"net/http"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllAgentFailure(t *testing.T) {
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
	}, 20*time.Second, 2*time.Second)

	require.Eventually(t, func() bool {
		resp, err := http.Get(c.WebEndpoints[rand.Intn(len(c.WebEndpoints))] + "/lock")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		data, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, lockHolder, string(data))
		return true
	}, 20*time.Second, 2*time.Second)

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
		require.Eventually(t, func() bool {
			t.Logf("endpoint: %s", endpoint)
			resp, err := http.Get(endpoint + "/lock")
			if err != nil {
				return false
			}
			defer resp.Body.Close()
			t.Logf("statusCode: %d", resp.StatusCode)
			if resp.StatusCode != http.StatusOK {
				return false
			}
			data, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			t.Logf("lockHolder: %s", string(data))
			require.Equal(t, lockHolder, string(data))
			return true
		}, 20*time.Second, 2*time.Second)
	}

	require.Eventually(t, func() bool {
		req, err := http.NewRequest(http.MethodPut,
			c.WebEndpoints[rand.Intn(len(c.WebEndpoints))]+"/unlock",
			bytes.NewBuffer([]byte(lockHolder)))
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		return resp.StatusCode == http.StatusOK
	}, 20*time.Second, 2*time.Second)
}
