package test

import (
	"bytes"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

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

	wg := sync.WaitGroup{}
	clientCount := 16
	wg.Add(clientCount)
	for i := 0; i < clientCount; i++ {
		go func(clientID int) {
			defer wg.Done()
			// Lock and check.
			lockHolder := strconv.Itoa(clientID)
			require.Eventually(t, func() bool {
				req, err := http.NewRequest(http.MethodPut,
					c.WebEndpoints[rand.Intn(len(c.WebEndpoints))]+"/lock",
					bytes.NewBuffer([]byte(lockHolder)))
				require.NoError(t, err)
				resp, err := http.DefaultClient.Do(req)
				require.NoError(t, err)
				return resp.StatusCode == http.StatusOK
			}, 3*time.Second, 10*time.Microsecond)

			require.Eventually(t, func() bool {
				resp, err := http.Get(c.WebEndpoints[rand.Intn(len(c.WebEndpoints))] + "/lock")
				require.NoError(t, err)
				if resp.StatusCode != http.StatusOK {
					return false
				}
				data, err = io.ReadAll(resp.Body)
				require.NoError(t, err)
				assert.Equal(t, lockHolder, string(data))
				return true
			}, 3*time.Second, 10*time.Microsecond)

			// Unlock.
			require.Eventually(t, func() bool {
				req, err := http.NewRequest(http.MethodPut,
					c.WebEndpoints[rand.Intn(len(c.WebEndpoints))]+"/unlock",
					bytes.NewBuffer([]byte(lockHolder)))
				require.NoError(t, err)
				resp, err := http.DefaultClient.Do(req)
				require.NoError(t, err)
				return http.StatusOK == resp.StatusCode
			}, 3*time.Second, 10*time.Microsecond)
		}(i)
	}
	wg.Wait()
}
