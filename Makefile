.PHONY: build
.PHONY: up
.PHONY: down
.PHONY: check-spark-server

build:
    docker compose build

up:
    docker compose up

down:
    docker compose down -v --remove-orphans

check-spark-server:
    docker compose exec spark-connect-client python /app/check_spark_server.py
