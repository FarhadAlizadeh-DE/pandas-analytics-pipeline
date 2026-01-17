up:
	docker compose up -d --build

down:
	docker compose down

pipeline:
	docker compose run --rm app python -m src.pipeline

load:
	docker compose run --rm app python scripts/load_to_postgres.py

metrics:
	docker compose exec -T db psql -U analytics -d analytics -f /app/sql/metrics.sql

reset:
	docker compose down -v
