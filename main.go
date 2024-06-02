package main

import (
	"log"
	"net/http"

	"github.com/peng225/starfish/internal/server"
)

func main() {
	http.HandleFunc("/lock", server.LockHandler)

	log.Fatal(http.ListenAndServe(":10080", nil))
}
