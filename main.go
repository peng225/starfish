package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/peng225/deduplog"
	"github.com/peng225/rlog"

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
		slog.Error("Invalid endpoint format.",
			slog.String("endpoint", endpoint))
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
	logger := slog.New(deduplog.NewDedupHandler(context.Background(),
		rlog.NewRawTextHandler(os.Stdout, &rlog.HandlerOptions{
			AddSource: true,
		}),
		&deduplog.HandlerOptions{
			HistoryRetentionPeriod: 1 * time.Second,
			MaxHistoryCount:        deduplog.DefaultMaxHistoryCount,
		}))
	slog.SetDefault(logger)

	var id int
	var configFileName string
	flag.IntVar(&id, "id", -1, "Agent ID")
	flag.StringVar(&configFileName, "config", "", "Config file name")
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
	err = http.ListenAndServe(":"+strconv.Itoa(webPort), nil)
	if err != nil {
		slog.Error("ListenAndServe failed.",
			slog.String("err", err.Error()))
		os.Exit(1)
	}
}
