package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/peng225/starfish/internal/agent"
	"github.com/peng225/starfish/internal/store"
	"github.com/peng225/starfish/internal/web"
	"gopkg.in/yaml.v2"
)

type config struct {
	WebEndpoints  []string `yaml:"webEndpoints"`
	GRPCEndpoints []string `yaml:"grpcEndpoints"`
}

func getPort(endpoint string) int {
	tokens := strings.Split(endpoint, ":")
	if len(tokens) != 2 && len(tokens) != 3 {
		log.Fatalf(`Invalid endpoint format "%s".`, endpoint)
	}
	port, err := strconv.Atoi(tokens[len(tokens)-1])
	if err != nil {
		log.Fatalf(`Failed to parse the gRPC port "%s". err: %s`,
			tokens, err)
	}
	return port
}

func main() {
	var id int
	var configFileName string
	flag.IntVar(&id, "id", -1, "Agent ID")
	flag.StringVar(&configFileName, "config", "", "Config file name")
	flag.Parse()

	if id < 0 {
		log.Fatalf("id must not be a negative number. id = %d", id)
	}

	data, err := os.ReadFile(configFileName)
	if err != nil {
		log.Fatalf(`Failed to open file "%s". err: %s`, configFileName, err)
	}
	c := config{}
	err = yaml.Unmarshal(data, &c)
	if err != nil {
		log.Fatalf("Failed to unmarshal the config file. err: %s", err)
	}
	fs := store.MustNewFileStore(fmt.Sprintf("filestore-%d.bin", id))
	agent.Init(int32(id), c.GRPCEndpoints, fs)

	// Start gRPC server.
	grpcPort := getPort(c.GRPCEndpoints[id])
	go agent.StartRPCServer(grpcPort)

	web.Init(c.WebEndpoints)

	// Start web server.
	http.HandleFunc("/lock", web.LockHandler)
	http.HandleFunc("/unlock", web.UnlockHandler)
	webPort := getPort(c.WebEndpoints[id])
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(webPort), nil))
}
