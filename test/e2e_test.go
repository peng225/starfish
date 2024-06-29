package test

import (
	"bytes"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/peng225/starfish/internal/agent"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

// TODO: duplicated in main.go
type config struct {
	WebServers   []string `yaml:"webServers"`
	GRPCServerss []string `yaml:"grpcServers"`
}

func readConfig(t *testing.T, fileName string) *config {
	t.Helper()
	data, err := os.ReadFile(fileName)
	require.NoError(t, err)
	c := config{}
	err = yaml.Unmarshal(data, &c)
	require.NoError(t, err)
	return &c
}

func checkLockHolder(t *testing.T, lockHolder int, server string) {
	t.Helper()
	require.Eventually(t, func() bool {
		resp, err := http.Get(server + "/lock")
		if err != nil {
			t.Logf("Failed to get the lock holder. lockHolder: %d, err: %s",
				lockHolder, err)
			return false
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Logf("HTTP status code is not OK. lockHolder: %d, statusCode: %d",
				lockHolder, resp.StatusCode)
			return false
		}
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Logf("Failed to read HTTP response body. lockHolder: %d, err: %s",
				lockHolder, err)
			return false
		}
		if strconv.Itoa(lockHolder) != string(data) {
			t.Logf("Unexpected lock holder. expected: %s, actual: %s",
				strconv.Itoa(lockHolder), string(data))
			return false
		}
		return true
	}, 30*time.Second, 100*time.Millisecond)
}

func lockRequest(t *testing.T, lockHolder int, server string) {
	t.Helper()
	require.Eventually(t, func() bool {
		req, err := http.NewRequest(http.MethodPut,
			server+"/lock",
			bytes.NewBuffer([]byte(strconv.Itoa(lockHolder))))
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Logf("Lock request failed. lockHolder: %d, err: %s",
				lockHolder, err)
			return false
		}
		return resp.StatusCode == http.StatusOK
	}, 60*time.Second, 100*time.Millisecond)
}

func unlockRequest(t *testing.T, lockHolder int, server string) {
	t.Helper()
	require.Eventually(t, func() bool {
		req, err := http.NewRequest(http.MethodPut,
			server+"/unlock",
			bytes.NewBuffer([]byte(strconv.Itoa(lockHolder))))
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Logf("Unlock request failed. lockHolder: %d, err: %s",
				lockHolder, err)
			return false
		}
		if resp.StatusCode != http.StatusOK {
			t.Logf("HTTP status code is not OK. lockHolder: %d, statusCode: %d",
				lockHolder, resp.StatusCode)
			return false
		}
		return true
	}, 30*time.Second, 100*time.Millisecond)
}

func TestLockAndUnlock(t *testing.T) {
	c := readConfig(t, "../config.yaml")

	// Check the initial status.
	checkLockHolder(t, int(agent.InvalidAgentID), c.WebServers[rand.Intn(len(c.WebServers))])

	// Lock and check.
	lockHolder1 := 1
	lockRequest(t, lockHolder1, c.WebServers[rand.Intn(len(c.WebServers))])
	checkLockHolder(t, lockHolder1, c.WebServers[rand.Intn(len(c.WebServers))])

	// Another client try to lock, but fails.
	lockHolder2 := 2
	req, err := http.NewRequest(http.MethodPut,
		c.WebServers[rand.Intn(len(c.WebServers))]+"/lock",
		bytes.NewBuffer([]byte(strconv.Itoa(lockHolder2))))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, resp.StatusCode)

	// Unlock and check.
	unlockRequest(t, lockHolder1, c.WebServers[rand.Intn(len(c.WebServers))])
	checkLockHolder(t, int(agent.InvalidLockHolderID), c.WebServers[rand.Intn(len(c.WebServers))])
}
