.PHONY: install test test-unit test-integration up down worker api

install:
	pip install -r requirements-dev.txt

test-unit:           # no Temporal server needed
	pytest -m "not integration"

test-integration:    # starts a local Temporal dev server automatically (first run downloads the CLI)
	pytest -m integration

test:
	pytest

up:
	docker compose up --build -d

down:
	docker compose down

worker:
	python -m wfengine.worker

api:
	uvicorn wfengine.api:app_factory --factory --port 8000
