.PHONY: help init up down restart logs psql test typecheck sql-lint lint format clean

help:
	@echo "Available commands:"
	@echo "  make init          Build the image and initialize the Airflow metadata database"
	@echo "  make up            Start Airflow (api-server, scheduler, dag-processor, postgres)"
	@echo "  make down          Stop all services"
	@echo "  make restart       Restart all services"
	@echo "  make logs          Follow logs from all services"
	@echo "  make psql          Open a psql shell inside the postgres container"
	@echo "  make test          Run unit tests with pytest"
	@echo "  make typecheck     Run ty type checker"
	@echo "  make sql-lint      Run sqruff SQL linter"
	@echo "  make lint          Run ruff linter"
	@echo "  make format        Run ruff formatter"
	@echo "  make clean         Stop services and remove postgres data"

init:
	@echo "Building image and initializing Airflow..."
	@docker compose build
	@docker compose run --rm airflow-init

up:
	@docker compose up

down:
	@docker compose down

restart:
	@docker compose restart

logs:
	@docker compose logs -f

psql:
	@docker compose exec postgres psql -U $${POSTGRES_USER:-airflow} -d $${POSTGRES_DB:-airflow}

test:
	@uv sync
	@uv run --env-file .env pytest tests -v

typecheck:
	@uv sync
	@uv run --env-file .env ty check dags tests

sql-lint:
	@uv sync
	@uv run sqruff lint dags

lint:
	@uv sync
	@uv run ruff check dags tests

format:
	@uv sync
	@uv run ruff format dags tests

clean:
	@docker compose down -v --remove-orphans
	@sudo rm -rf data/postgres logs/*
	@echo "Cleaned. Run 'make init && make up' to start fresh."
