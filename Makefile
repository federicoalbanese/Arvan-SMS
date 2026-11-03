# Integration tests use their own database (sms_test) and Redis DBs 14/15,
# so they never mix with the data of a running stack.
TEST_DATABASE_URL ?= postgres://sms:sms@localhost:5432/sms_test?sslmode=disable
TEST_REDIS_ADDR   ?= localhost:6379

.PHONY: build test test-integration lint up down logs loadgen

## build: compile all binaries into ./bin
build:
	go build -o bin/ ./cmd/...

## test: unit tests (no external services needed)
test:
	go test -race ./...

## test-integration: all tests against the Postgres and Redis from docker compose
## (-p 1: packages share the test database, so run them one after another)
test-integration:
	docker compose up -d postgres redis --wait
	docker compose exec -T postgres psql -U sms -tc "SELECT 1 FROM pg_database WHERE datname = 'sms_test'" | grep -q 1 \
		|| docker compose exec -T postgres createdb -U sms sms_test
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" TEST_REDIS_ADDR="$(TEST_REDIS_ADDR)" go test -race -count=1 -p 1 ./...

## lint: static checks
lint:
	gofmt -l . | (! grep .) || (echo "run gofmt -w ." && exit 1)
	go vet ./...

## up: start the whole stack
up:
	docker compose up -d --build --wait

## down: stop the stack and delete its data
down:
	docker compose down -v

## logs: follow api and worker logs
logs:
	docker compose logs -f api worker

## loadgen: fairness and express SLA demo against the running stack
loadgen:
	go run ./cmd/loadgen
