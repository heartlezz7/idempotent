.PHONY: up down run tidy test psql reset swagger

up:            ## start postgres and apply migrations (the app runs on the host via `make run`)
	docker compose up -d db
	@echo "waiting for postgres..."
	@until docker compose exec -T db pg_isready -U postgres -d postgres >/dev/null 2>&1; do sleep 1; done
	@echo "ready"

down:          ## stop and wipe the database volume
	docker compose down -v

tidy:
	go mod tidy

run:
	go run ./cmd/server/**.go

test:          ## run the full experiment suite against a running server
	./scripts/experiments.sh

reset:
	curl -sX POST localhost:8080/_reset | jq .

psql:
	docker compose exec db psql -U postgres -d postgres

SWAG_VERSION ?= v1.16.6

swagger:       ## regenerate docs/swagger from the handler annotations
	go run github.com/swaggo/swag/cmd/swag@$(SWAG_VERSION) fmt -g main.go -d ./cmd/server,./src/handler
	go run github.com/swaggo/swag/cmd/swag@$(SWAG_VERSION) init -g main.go -d ./cmd/server,./src/handler,./src/model -o docs/swagger --parseInternal
