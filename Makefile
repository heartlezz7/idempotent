.PHONY: up down run tidy test unit-test psql reset swagger

up:            ## start postgres and apply migrations
	docker compose up -d db
	@echo "waiting for postgres..."
	@until docker compose exec -T db pg_isready -U postgres -d postgres >/dev/null 2>&1; do sleep 1; done
	@echo "ready"

down:          ## stop and wipe the database volume
	docker compose down -v

tidy:
	go mod tidy

run:      ## start the API on :8080
	go run ./cmd/server

test:          ## run the full experiment suite against a running server (needs `make run` in another shell)
	./scripts/experiments.sh

unit-test:     ## run the go test suite directly against postgres (needs `make up`, no server required)
	go test ./... -count=1

psql:
	docker compose exec db psql -U postgres -d postgres

reset:
	curl -sX POST localhost:8080/_reset | jq .

SWAG_VERSION ?= v1.16.6

swagger:       ## regenerate docs/swagger from the handler annotations
	go run github.com/swaggo/swag/cmd/swag@$(SWAG_VERSION) fmt -g main.go -d ./cmd/server,./src/handler
	go run github.com/swaggo/swag/cmd/swag@$(SWAG_VERSION) init -g main.go -d ./cmd/server,./src/handler,./src/model,./src/consumer -o docs/swagger --parseInternal
