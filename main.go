package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/peng225/rlog"

	"github.com/peng225/starfish/internal/agent"
	"github.com/peng225/starfish/internal/store"
	"github.com/peng225/starfish/internal/web"
	"gopkg.in/yaml.v2"
)

type config struct {
	WebServers  []string `yaml:"webServers"`
	GRPCServers []string `yaml:"grpcServers"`
}

func getPort(server string) int {
	tokens := strings.Split(server, ":")
	if len(tokens) != 2 && len(tokens) != 3 {
		slog.Error("Invalid server format.",
			slog.String("server", server))
		os.Exit(1)
	}
	port, err := strconv.Atoi(tokens[len(tokens)-1])
	if err != nil {
		slog.Error("Failed to parse the gRPC port", "tokens", tokens,
			slog.String("err", err.Error()))
		os.Exit(1)
	}
	return port
}

func main() {
	// Set up logger.
	logger := slog.New(rlog.NewRawTextHandler(os.Stdout, &rlog.HandlerOptions{
		AddSource: true,
	}))
	slog.SetDefault(logger)

	var id int
	var configFileName string
	var pstoreDir string
	flag.IntVar(&id, "id", -1, "Agent ID")
	flag.StringVar(&configFileName, "config", "", "Config file name")
	flag.StringVar(&pstoreDir, "pstore-dir", "", "Persistent store directory")
	flag.Parse()

	if id < 0 {
		slog.Error("id must not be a negative number.",
			slog.Int("id", id))
		os.Exit(1)
	}

	data, err := os.ReadFile(configFileName)
	if err != nil {
		slog.Error("Failed to open file.",
			slog.String("fileName", configFileName),
			slog.String("err", err.Error()))
		os.Exit(1)
	}
	c := config{}
	err = yaml.Unmarshal(data, &c)
	if err != nil {
		slog.Error("Failed to unmarshal the config file.",
			slog.String("err", err.Error()))
		os.Exit(1)
	}
	pstoreFileFullPath := path.Join(pstoreDir, fmt.Sprintf("filestore-%d.bin", id))
	fs := store.MustNewFileStore(pstoreFileFullPath)
	agent.Init(int32(id), c.GRPCServers, fs)

	// Start gRPC server.
	grpcPort := getPort(c.GRPCServers[id])
	go agent.StartRPCServer(grpcPort)

	web.Init(c.WebServers)

	// Start web server.
	http.HandleFunc("/lock", web.LockHandler)
	http.HandleFunc("/unlock", web.UnlockHandler)
	webPort := getPort(c.WebServers[id])
	err = http.ListenAndServe(":"+strconv.Itoa(webPort), nil)
	if err != nil {
		slog.Error("ListenAndServe failed.",
			slog.String("err", err.Error()))
		os.Exit(1)
	}
}
