GO ?= go
.PHONY: build unit race integration generator generator-integration modules
build:
	$(GO) build -mod=readonly ./...
unit:
	$(GO) test ./pkg/pi/... ./cmd/pi
race:
	$(GO) test -race ./pkg/pi ./pkg/pi/http/... ./pkg/pi/cli/... ./pkg/pi/config ./pkg/pi/datasource/sql/... ./pkg/pi/migration
integration:
	$(GO) test ./pkg/pi/migration -count=1
generator:
	$(GO) test ./pkg/pi/cli/... -count=1
generator-integration:
	./scripts/test-generator.sh
# Independent adapter modules need their own module graph; root ./... excludes them.
modules:
	./scripts/check-modules.sh
