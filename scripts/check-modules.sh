#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$PWD"
while IFS= read -r module; do
  directory="${module%/go.mod}"
  echo "Building independent module $directory"
  (cd "$directory" && GOWORK=off go build -mod=readonly ./...)
done < <(find "$ROOT/pkg/pi/datasource" -name go.mod | sort)
