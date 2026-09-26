#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN="$(mktemp -d "${TMPDIR:-/tmp}/pi-generator-test.XXXXXX")"
trap 'rm -rf "$RUN"' EXIT
[[ "$(protoc --version)" == 'libprotoc 33.1' ]]
[[ "$(protoc-gen-go --version)" == 'protoc-gen-go v1.28.0' ]]
[[ "$(protoc-gen-go-grpc --version)" == 'protoc-gen-go-grpc 1.2.0' ]]
cd "$ROOT"
GOWORK=off go build -o "$RUN/pi" ./cmd/pi
VERSION="$("$RUN/pi" --version | awk '{print $3}')"
mkdir -p "$RUN/project/fixture"
cp go.mod go.sum "$RUN/project/"
cd "$RUN/project"
# This checks the local candidate. Published-tag consumption is a separate check.
GOWORK=off go mod edit -module=example.com/pi-generator-test -require="github.com/sllt/pi@$VERSION" -replace="github.com/sllt/pi=$ROOT"
cat > fixture/contract.proto <<'PROTO'
syntax = "proto3";
package fixture.v1;
option go_package = "example.com/pi-generator-test/fixture";
message Request { string value = 1; }
message Response { string value = 1; }
service Alpha {
  rpc Unary(Request) returns (Response);
  rpc Server(Request) returns (stream Response);
  rpc Client(stream Request) returns (Response);
  rpc Bidi(stream Request) returns (stream Response);
}
service Beta {
  rpc Unary(Request) returns (Response);
  rpc Server(Request) returns (stream Response);
  rpc Client(stream Request) returns (Response);
  rpc Bidi(stream Request) returns (stream Response);
}
PROTO
protoc --go_out=. --go_opt=module=example.com/pi-generator-test --go-grpc_out=. --go-grpc_opt=module=example.com/pi-generator-test fixture/contract.proto
"$RUN/pi" wrap grpc server --proto fixture/contract.proto --out fixture
"$RUN/pi" wrap grpc client --proto fixture/contract.proto --out fixture
BEFORE="$(shasum fixture/alpha_server.go fixture/beta_server.go)"
"$RUN/pi" wrap grpc server --proto fixture/contract.proto --out fixture
[[ "$(shasum fixture/alpha_server.go fixture/beta_server.go)" == "$BEFORE" ]]
GOWORK=off go test -mod=mod ./...
echo 'generator integration passed: two services, unary/all streaming shapes, regeneration and compilation'
