# iceberg-sandbox

# FIXME
将来的にpostgresql-xxx.jarはDockerfileで取得する形にする。
現在は、iceberg-rest-fixtureイメージにPostgreSQLのJDBCドライバが含まれていないので、ローカルにダウンロードしてボリュームマウントしている。
```
curl -o ./postgresql-42.7.5.jar https://jdbc.postgresql.org/download/postgresql-42.7.5.jar
```
