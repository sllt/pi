#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$PWD"
while IFS= read -r module; do
  directory="${module%/go.mod}"
  echo "Building independent module $directory"
  (cd "$directory" && GOWORK=off go build -mod=readonly ./...)
done < <(find "$ROOT/pkg/pi/datasource" -name go.mod | sort)

if [[ -f "$ROOT/internal/openapiverify/go.mod" ]]; then
  echo "Checking independent test module $ROOT/internal/openapiverify"
  (cd "$ROOT/internal/openapiverify" && GOWORK=off go test -mod=readonly -run='^$' ./...)
fi
