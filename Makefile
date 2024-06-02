STARFISH := ./starfish
GO_FILES := $(shell find . -path './test' -prune -o -type f -name '*.go' -print)

$(STARFISH): $(GO_FILES)
	CGO_ENABLED=0 go build -o $@ -v

.PHONY: proto
proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		internal/rpc/raft.proto

.PHONY: clean
clean:
	rm -f $(STARFISH)