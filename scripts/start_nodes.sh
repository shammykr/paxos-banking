#!/usr/bin/env bash
# Starts nodes 1-3 in the background; logs go to logs/nodeN.log.
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p logs
for id in 1 2 3; do
  ./bin/node -id "$id" -config config/nodes.yaml > "logs/node$id.log" 2>&1 &
  echo "started node $id (pid $!)"
done
