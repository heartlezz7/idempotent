.PHONY: up down run tidy test psql reset swagger

up:            ## start postgres and apply migrations
	docker compose up -d
	@echo "waiting for postgres..."
	@until docker compose exec -T db pg_isready -U lab -d lab >/dev/null 2>&1; do sleep 1; done
	@echo "ready"

down:          ## stop and wipe the database volume
	docker compose down -v

tidy:
	go mod tidy

run: tidy      ## start the API on :8080
	go run ./cmd/server

test:          ## run the full experiment suite against a running server
	./scripts/experiments.sh

reset:
	curl -sX POST localhost:8080/_reset | jq .

psql:
	docker compose exec db psql -U postgres -d postgres

SWAG_VERSION ?= v1.16.6

swagger:       ## regenerate docs/swagger from the handler annotations
	go run github.com/swaggo/swag/cmd/swag@$(SWAG_VERSION) fmt -g main.go -d ./,./src/handler
	go run github.com/swaggo/swag/cmd/swag@$(SWAG_VERSION) init -g main.go -d ./,./src/handler,./src/model -o docs/swagger --parseInternal
