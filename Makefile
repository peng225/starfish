STARFISH := ./starfish
GO_FILES := $(shell find . -path './test' -prune -o -type f -name '*.go' -print)

$(STARFISH): $(GO_FILES)
	CGO_ENABLED=0 go build -o $@ -v

.PHONY: proto
proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		internal/rpc/raft.proto

.PHONY: run
run: $(STARFISH)
	$(STARFISH) -id 0 -config config.yaml -pstore-dir /tmp &
	$(STARFISH) -id 1 -config config.yaml -pstore-dir /tmp &
	$(STARFISH) -id 2 -config config.yaml -pstore-dir /tmp &

.PHONY: test
test: $(STARFISH)
	go test -v ./internal/...

.PHONY: e2e-test
e2e-test: $(STARFISH)
	go test -v -timeout 3m -failfast ./test/...

.PHONY: clean
clean:
	pkill $$(basename $(STARFISH)) || true
	rm -f $(STARFISH)
