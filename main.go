package main

import (
	"flag"
	"log"
	"net/http"
	"strconv"

	"github.com/peng225/starfish/internal/agent"
	"github.com/peng225/starfish/internal/server"
)

func main() {
	var id int
	var serverPort int
	var grpcPortOffset int
	flag.IntVar(&id, "id", -1, "Agent ID")
	flag.IntVar(&serverPort, "port", 10080, "Server port number")
	flag.IntVar(&grpcPortOffset, "grpc-port-offset", 8080, "The offset of port numbers for gRPC")
	flag.Parse()

	if id < 0 {
		log.Fatalf("id must not be a negative number. id = %d", id)
	}
	if grpcPortOffset < 1024 {
		log.Fatalf("grpcPortOffset must not be a well known port. grpcPortOffset = %d", grpcPortOffset)
	}

	go agent.StartFollower(grpcPortOffset + id)

	http.HandleFunc("/lock", server.LockHandler)
	http.HandleFunc("/unlock", server.UnlockHandler)
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(serverPort), nil))
}
