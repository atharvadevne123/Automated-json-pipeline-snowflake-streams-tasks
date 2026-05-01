.PHONY: install test lint lint-fix run docker-build clean

install:
	pip install -e ".[dev]"

test:
	python -m pytest -v --tb=short

test-cov:
	python -m pytest -v --tb=short --cov=snowflake_pipeline --cov-report=term-missing --cov-report=html

lint:
	python -m ruff check . --select E,F,W,I --ignore E501

lint-fix:
	python -m ruff check . --select E,F,W,I --ignore E501 --fix

format:
	python -m ruff format .

typecheck:
	python -m mypy snowflake_pipeline --ignore-missing-imports

docker-build:
	docker build -t snowflake-json-pipeline:latest .

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name htmlcov -exec rm -rf {} + 2>/dev/null || true
	rm -f .coverage
