.PHONY: up down run tidy test psql reset

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
