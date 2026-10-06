.PHONY: build test test-race run compose-up compose-down fmt vet

build:
	go build ./...

test:
	go test ./...

test-race:
	go test -race -count=1 ./...

run:
	go run ./cmd/server

compose-up:
	docker compose -f deploy/docker-compose.yml up --build

compose-down:
	docker compose -f deploy/docker-compose.yml down

fmt:
	gofmt -w .

vet:
	go vet ./...
