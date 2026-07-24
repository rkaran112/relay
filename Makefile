# relay task runner. Recipes shell out to uv; override with `make UV="python -m uv"`
# on machines where the uv shim isn't on PATH (e.g. Windows without a uv.exe).
UV ?= uv
SESSIONS ?= 500

.PHONY: help up down migrate test lint typecheck fmt check bench chaos

help:
	@echo "relay targets:"
	@echo "  make up         start Postgres + Redis (docker compose)"
	@echo "  make down       stop and remove containers"
	@echo "  make migrate    apply Alembic migrations"
	@echo "  make test       run the test suite against real Postgres"
	@echo "  make lint       ruff check"
	@echo "  make typecheck  mypy --strict"
	@echo "  make check      lint + typecheck + test"
	@echo "  make bench      load benchmark            (Phase 8)"
	@echo "  make chaos      chaos harness, SESSIONS=N  (Phase 9)"

up:
	docker compose up -d db redis

down:
	docker compose down

migrate:
	$(UV) run relay migrate

test:
	$(UV) run pytest -q

lint:
	$(UV) run ruff check .

typecheck:
	$(UV) run mypy relay

fmt:
	$(UV) run ruff format .

check: lint typecheck test

bench:
	@echo "bench harness lands in Phase 8 (bench/load.py)"

chaos:
	@echo "chaos harness lands in Phase 9 (chaos/runner.py), SESSIONS=$(SESSIONS)"
