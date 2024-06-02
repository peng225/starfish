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
	flag.IntVar(&id, "id", -1, "Agent ID")
	flag.IntVar(&serverPort, "port", 10080, "Server port number")
	flag.Parse()

	// TODO: set correct port.
	go agent.StartFollower(8080)

	http.HandleFunc("/lock", server.LockHandler)
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(serverPort), nil))
}
