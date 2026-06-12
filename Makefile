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
        proto \
        test test-unit test-integration \
        lint vet fmt \
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

test-e2e:
	go test ./test/e2e/... -count=1 -timeout 300s

# ── Quality ──────────────────────────────────────────────────────────────────

lint:
	golangci-lint run ./...

vet:
	go vet ./...

fmt:
	gofmt -l -w .

check: vet lint

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
	curl -s -X POST http://localhost:8081/v1/admin/reload | jq .

can-stop:
	curl -s http://localhost:8081/v1/admin/can-stop | jq .

# ── Kubernetes ───────────────────────────────────────────────────────────────

k8s-apply:
	kubectl apply -f deploy/k8s/

k8s-delete:
	kubectl delete -f deploy/k8s/
