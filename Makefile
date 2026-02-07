.PHONY: build up down restart logs test lint clean

# ─── Docker Commands ─────────────────────────────────────────
build:
	docker-compose build

up:
	docker-compose up -d
	@echo "Services started. Access:"
	@echo "  Airflow:    http://localhost:8080 (admin/admin)"
	@echo "  Frontend:   http://localhost:5000"
	@echo "  Spark UI:   http://localhost:8081"

down:
	docker-compose down

restart:
	docker-compose down && docker-compose up -d

logs:
	docker-compose logs -f

logs-airflow:
	docker-compose logs -f airflow-scheduler airflow-webserver

logs-frontend:
	docker-compose logs -f frontend

# ─── Database ────────────────────────────────────────────────
init-db:
	docker-compose exec postgres-warehouse psql -U pipeline_user -d review_warehouse -f /docker-entrypoint-initdb.d/01_create_warehouse.sql

# ─── Testing ─────────────────────────────────────────────────
test:
	pytest tests/ -v --tb=short

test-cov:
	pytest tests/ -v --cov=scripts --cov=frontend --cov-report=html --cov-report=term-missing

# ─── Code Quality ────────────────────────────────────────────
lint:
	flake8 scripts/ dags/ frontend/ --max-line-length=120 --ignore=E501
	black --check scripts/ dags/ frontend/

format:
	black scripts/ dags/ frontend/

# ─── Utilities ───────────────────────────────────────────────
clean:
	docker-compose down -v --remove-orphans
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf htmlcov .coverage .pytest_cache

shell-airflow:
	docker-compose exec airflow-webserver bash

shell-mongo:
	docker-compose exec mongo mongosh restaurant_reviews

shell-postgres:
	docker-compose exec postgres-warehouse psql -U pipeline_user -d review_warehouse

# ─── Spark Jobs ──────────────────────────────────────────────
spark-submit:
	docker-compose exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark-jobs/review_processing_job.py
