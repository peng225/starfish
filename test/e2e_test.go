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
	WebEndpoints  []string `yaml:"webEndpoints"`
	GRPCEndpoints []string `yaml:"grpcEndpoints"`
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
			t.Logf("Failed to get the lock holder. err: %s", err)
			return false
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Logf("HTTP status code is not OK. statusCode: %d", resp.StatusCode)
			return false
		}
		data, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return strconv.Itoa(lockHolder) == string(data)
	}, 20*time.Second, 2*time.Second)
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
			t.Logf("Lock request failed. err: %s", err)
			return false
		}
		return resp.StatusCode == http.StatusOK
	}, 20*time.Second, 2*time.Second)
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
			t.Logf("Unlock request failed. err: %s", err)
			return false
		}
		return resp.StatusCode == http.StatusOK
	}, 20*time.Second, 2*time.Second)
}

func TestLockAndUnlock(t *testing.T) {
	c := readConfig(t, "../config.yaml")

	// Check the initial status.
	checkLockHolder(t, int(agent.InvalidAgentID), c.WebEndpoints[rand.Intn(len(c.WebEndpoints))])

	// Lock and check.
	lockHolder1 := 1
	lockRequest(t, lockHolder1, c.WebEndpoints[rand.Intn(len(c.WebEndpoints))])
	checkLockHolder(t, lockHolder1, c.WebEndpoints[rand.Intn(len(c.WebEndpoints))])

	// Another client try to lock, but fails.
	lockHolder2 := 2
	req, err := http.NewRequest(http.MethodPut,
		c.WebEndpoints[rand.Intn(len(c.WebEndpoints))]+"/lock",
		bytes.NewBuffer([]byte(strconv.Itoa(lockHolder2))))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, resp.StatusCode)

	// Unlock and check.
	unlockRequest(t, lockHolder1, c.WebEndpoints[rand.Intn(len(c.WebEndpoints))])
	checkLockHolder(t, int(agent.InvalidLockHolderID), c.WebEndpoints[rand.Intn(len(c.WebEndpoints))])
}
