#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p bin
go build -o bin/node ./cmd/node
go build -o bin/client ./cmd/client
echo "built: bin/node bin/client"
