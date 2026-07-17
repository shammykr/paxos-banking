# Paxos Bank

A distributed banking system built on Multi-Paxos consensus with Two-Phase Commit
for cross-shard transactions. Written in Go as a from-scratch learning project.

Reference material: [Paxos Made Simple (Lamport)](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf).

## Setup

```bash
# 1. Install Go 1.22+ (https://go.dev/dl) and protoc
brew install go protobuf        # macOS
# apt install golang-go protobuf-compiler   # Linux

# 2. Install protoc Go plugins
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
export PATH="$PATH:$(go env GOPATH)/bin"

# 3. Generate protobuf code + fetch deps
./scripts/gen_proto.sh
go mod tidy

# 4. Build and run
./scripts/build.sh
./scripts/start_nodes.sh
./bin/client
```

## Design

Consensus logic lives in `internal/paxos/` and is deliberately **pure** (no
networking), so the algorithm is unit-testable without gRPC:

```bash
go test -race ./internal/paxos/ -v
```

`internal/node/` is the gRPC shell that exposes the same logic over the
network. Tests are written up front against the core, TDD-style.

## Roadmap

- [x] Project skeleton, proto definitions, gRPC plumbing
- [ ] Single-decree Paxos (acceptor + proposer)
- [ ] Multi-Paxos replicated log + state machine
- [ ] Leader election & heartbeats
- [ ] Write-ahead log & crash recovery
- [ ] Second shard + 2PC cross-shard transactions
- [ ] Benchmarks, chaos tests, docs

Future work: checkpointing, shard redistribution, PebbleDB-backed storage,
configurable cluster topology.
