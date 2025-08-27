.PHONY: build
.PHONY: up
.PHONY: down
.PHONY: spark-connect-server-log
.PHONY: spark-connect-client

build:
    docker-compose build

up:
    docker-compose up

down:
    docker-compose down -v --remove-orphans

spark-connect-server-log:
    # spark-connect-serverが正常起動できたかは以下を実行して確認
    docker-compose exec spark-connect-server tail -f /opt/spark/logs/spark--org.apache.spark.sql.connect.service.SparkConnectServer-1-spark-connect-server.out

spark-connect-client:
    # spark-connect-clientに入る方法
    docker-compose exec spark-connect-client /bin/bash
