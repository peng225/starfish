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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

// TODO: duplicated in main.go
type config struct {
	WebEndpoints  []string `yaml:"webEndpoints"`
	GRPCEndpoints []string `yaml:"grpcEndpoints"`
}

// TODO: "Eventually" is needed for PUT requests, too.
func TestLockAndUnlock(t *testing.T) {
	configFileName := "../config.yaml"
	data, err := os.ReadFile(configFileName)
	require.NoError(t, err)
	c := config{}
	err = yaml.Unmarshal(data, &c)
	require.NoError(t, err)

	// Check the initial status.
	require.Eventually(t, func() bool {
		resp, err := http.Get(c.WebEndpoints[rand.Intn(len(c.WebEndpoints))] + "/lock")
		require.NoError(t, err)
		if resp.StatusCode != http.StatusOK {
			return false
		}
		data, err = io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, strconv.Itoa(int(agent.InvalidLockHolderID)), string(data))
		return true
	}, 3*time.Second, 10*time.Microsecond)

	// Lock and check.
	lockHolder1 := strconv.Itoa(1)
	req, err := http.NewRequest(http.MethodPut,
		c.WebEndpoints[rand.Intn(len(c.WebEndpoints))]+"/lock",
		bytes.NewBuffer([]byte(lockHolder1)))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Eventually(t, func() bool {
		resp, err := http.Get(c.WebEndpoints[rand.Intn(len(c.WebEndpoints))] + "/lock")
		require.NoError(t, err)
		if resp.StatusCode != http.StatusOK {
			return false
		}
		data, err = io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, lockHolder1, string(data))
		return true
	}, 3*time.Second, 10*time.Microsecond)

	// Another client try to lock, but fails.
	lockHolder2 := strconv.Itoa(2)
	req, err = http.NewRequest(http.MethodPut,
		c.WebEndpoints[rand.Intn(len(c.WebEndpoints))]+"/lock",
		bytes.NewBuffer([]byte(lockHolder2)))
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, resp.StatusCode)

	// Unlock and check.
	req, err = http.NewRequest(http.MethodPut,
		c.WebEndpoints[rand.Intn(len(c.WebEndpoints))]+"/unlock",
		bytes.NewBuffer([]byte(lockHolder1)))
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Eventually(t, func() bool {
		resp, err := http.Get(c.WebEndpoints[rand.Intn(len(c.WebEndpoints))] + "/lock")
		require.NoError(t, err)
		if resp.StatusCode != http.StatusOK {
			return false
		}
		data, err = io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, strconv.Itoa(int(agent.InvalidLockHolderID)), string(data))
		return true
	}, 3*time.Second, 10*time.Microsecond)
}
