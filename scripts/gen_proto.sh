#!/usr/bin/env bash
# Generates Go code from proto/paxos.proto into proto/paxospb/.
#
# One-time setup:
#   brew install protobuf            # or: apt install protobuf-compiler
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
#   export PATH="$PATH:$(go env GOPATH)/bin"
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p proto/paxospb
protoc \
  --go_out=proto/paxospb --go_opt=paths=source_relative \
  --go-grpc_out=proto/paxospb --go-grpc_opt=paths=source_relative \
  --proto_path=proto \
  proto/paxos.proto

echo "generated: proto/paxospb/"
