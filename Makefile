BINARY        := dbbridge
CMD           := ./cmd/dbbridge
BUILD_DIR     := bin
DOCKER_IMAGE  := dbbridge
DOCKER_TAG    := latest
COMPOSE_FILE  := deploy/docker-compose.yaml
CONFIG        := configs/dbbridge-blue.yaml

LDFLAGS       := -ldflags="-w -s"
GO_BUILD      := CGO_ENABLED=0 go build $(LDFLAGS)

.PHONY: all build clean run \
        proto proto-lint \
        test test-unit test-integration test-containers test-e2e test-all test-race \
        vulncheck \
        lint vet fmt fmt-check check ci \
        docker-build docker-push \
        up down logs restart \
        reload-config can-stop \
        k8s-apply k8s-delete

# ── Build ────────────────────────────────────────────────────────────────────

all: build

build:
	mkdir -p $(BUILD_DIR)
	$(GO_BUILD) -o $(BUILD_DIR)/$(BINARY) $(CMD)

clean:
	rm -rf $(BUILD_DIR)
	go clean -cache

run: build
	./$(BUILD_DIR)/$(BINARY) -config $(CONFIG)

# ── Proto ────────────────────────────────────────────────────────────────────

proto:
	buf generate

proto-lint:
	buf lint

# ── Tests ────────────────────────────────────────────────────────────────────

test: test-unit

test-unit:
	go test ./internal/... -short -count=1

test-integration:
	go test ./internal/... -count=1 -timeout 120s

# Real backends in containers (Redis, PostgreSQL, MySQL, MinIO, ClickHouse).
# Requires a Docker daemon; skipped by every other target via the build tag.
#
# testcontainers does not read the docker CLI context, so under colima it has to
# be told twice: DOCKER_HOST is the host-side socket it connects to, and
# TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE is the same socket as seen *inside* the
# VM, which is what its reaper container bind-mounts. Empty on Docker Desktop
# and in CI, where the default socket is already correct for both.
ifeq ($(shell docker context show 2>/dev/null),colima)
CONTAINER_ENV := DOCKER_HOST=$(shell docker context inspect colima --format '{{.Endpoints.docker.Host}}') \
                 TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
endif

test-containers:
	$(CONTAINER_ENV) go test -race -tags=integration ./test/integration/... -count=1 -timeout 900s

test-e2e:
	go test ./test/e2e/... -count=1 -timeout 300s

# Same scope as CI: every package, including test/e2e.
test-all:
	go test ./... -count=1

test-race:
	go test -race ./... -count=1

# ── Quality ──────────────────────────────────────────────────────────────────

# Pinned rather than @latest: an unpinned tool version makes CI fail on a
# release of the tool with no change in this repository, and runs unreviewed
# code from the network on every build.
GOVULNCHECK_VERSION ?= v1.1.4

vulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

lint:
	golangci-lint run ./...

# The integration package is cut out by its build tag, and a package whose files
# are all cut out is skipped by ./... without a word, so those files would
# otherwise not be vetted or compiled outside the container job.
vet:
	go vet ./...
	go vet -tags=integration ./test/integration/...

fmt:
	gofmt -l -w .

# Read-only counterpart of `fmt`: reports unformatted files and fails.
fmt-check:
	@out="$$(gofmt -l .)"; test -z "$$out" || { echo "$$out"; exit 1; }

check: vet lint

# The CI workflow runs these same targets, one per step, plus test-containers in
# a job of its own - that one needs a Docker daemon, so it is not part of `ci`.
ci: fmt-check vet test-all test-race lint proto-lint vulncheck

# ── Docker ───────────────────────────────────────────────────────────────────

docker-build:
	docker build -f deploy/Dockerfile -t $(DOCKER_IMAGE):$(DOCKER_TAG) .

docker-push:
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)

# ── Compose (dev) ────────────────────────────────────────────────────────────

up:
	docker compose -f $(COMPOSE_FILE) up -d --build

down:
	docker compose -f $(COMPOSE_FILE) down

logs:
	docker compose -f $(COMPOSE_FILE) logs -f

restart:
	docker compose -f $(COMPOSE_FILE) restart dbbridge-blue dbbridge-green

# ── Admin endpoints ──────────────────────────────────────────────────────────

reload-config:
	curl -s -X POST http://localhost:8181/v1/admin/reload \
		-H "Authorization: Bearer $${DBBRIDGE_TOKEN_ADMIN:-dev-admin-token}" | jq .

can-stop:
	curl -s http://localhost:8181/v1/admin/can-stop \
		-H "Authorization: Bearer $${DBBRIDGE_TOKEN_ADMIN:-dev-admin-token}" | jq .

# ── Kubernetes ───────────────────────────────────────────────────────────────

k8s-apply:
	kubectl apply -f deploy/k8s/

k8s-delete:
	kubectl delete -f deploy/k8s/
