package test

import (
	"bytes"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"testing"

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

func TestStress(t *testing.T) {
	configFileName := "../config.yaml"
	data, err := os.ReadFile(configFileName)
	if err != nil {
		log.Fatalf(`Failed to open file "%s". err: %s`, configFileName, err)
	}
	c := config{}
	err = yaml.Unmarshal(data, &c)
	if err != nil {
		log.Fatalf("Failed to unmarshal the config file. err: %s", err)
	}

	// Get the lock holder.
	resp, err := http.Get(c.WebEndpoints[rand.Intn(len(c.WebEndpoints))] + "/lock")
	require.NoError(t, err)
	data, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, strconv.Itoa(int(agent.InvalidLockHolderID)), string(data))

	// Lock and check.
	lockHolder := "1"
	req, err := http.NewRequest(http.MethodPut,
		c.WebEndpoints[rand.Intn(len(c.WebEndpoints))]+"/lock",
		bytes.NewBuffer([]byte(lockHolder)))
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)

	resp, err = http.Get(c.WebEndpoints[rand.Intn(len(c.WebEndpoints))] + "/lock")
	require.NoError(t, err)
	data, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, lockHolder, string(data))

	// Unlock and check.
	req, err = http.NewRequest(http.MethodPut,
		c.WebEndpoints[rand.Intn(len(c.WebEndpoints))]+"/unlock",
		bytes.NewBuffer([]byte("1")))
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)

	resp, err = http.Get(c.WebEndpoints[rand.Intn(len(c.WebEndpoints))] + "/lock")
	require.NoError(t, err)
	data, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, strconv.Itoa(int(agent.InvalidLockHolderID)), string(data))
}
