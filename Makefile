.PHONY: build
.PHONY: up
.PHONY: down
.PHONY: smoketest

build:
    docker compose build

up:
    docker compose up

down:
    docker compose down -v --remove-orphans

smoketest:
    docker compose exec spark-connect-client python /app/iceberg_smoketest.py

