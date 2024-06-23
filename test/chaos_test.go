package test

import (
	"log"
	"math/rand"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func runClinetsForever(t *testing.T, done chan struct{}) {
	t.Helper()
	c := readConfig(t, "../config.yaml")
	clientCount := 16
	for {
		select {
		case <-done:
			return
		default:
		}
		wg := sync.WaitGroup{}
		wg.Add(clientCount)
		for i := 0; i < clientCount; i++ {
			go func(clientID int) {
				defer wg.Done()
				lockRequest(t, clientID, c.WebEndpoints[rand.Intn(len(c.WebEndpoints))])
				checkLockHolder(t, clientID, c.WebEndpoints[rand.Intn(len(c.WebEndpoints))])
				unlockRequest(t, clientID, c.WebEndpoints[rand.Intn(len(c.WebEndpoints))])
			}(i)
		}
		wg.Wait()
	}
}

func TestChaos(t *testing.T) {
	done := make(chan struct{})
	go runClinetsForever(t, done)

	cmd := exec.Command("pidof", "starfish")
	var out strings.Builder
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		log.Fatal(err)
	}

	pids := getAgentPIDs(t)
	for i := 0; i < 3; i++ {
		for _, pid := range pids {
			time.Sleep(time.Duration(1+rand.Intn(9)) * time.Second)
			sendSignal(t, syscall.SIGSTOP, pid)
			time.Sleep(time.Duration(1+rand.Intn(9)) * time.Second)
			sendSignal(t, syscall.SIGCONT, pid)
		}
	}
	done <- struct{}{}
}
