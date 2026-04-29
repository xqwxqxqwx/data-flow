# data-flow

Пет‑проект по data engineering в Docker: **Airflow + Spark + Kafka + S3(MinIO) + Iceberg + Postgres + ClickHouse + dbt**.

## Что уже работает (MVP)

- **Источник**: `postgres-source` с демо-таблицей `raw.orders`
- **Lakehouse**: Spark пишет таблицу **Iceberg** в MinIO (`s3a://warehouse/iceberg/...`)
- **DWH**: данные грузятся в ClickHouse в таблицу `analytics.raw_orders`
- **dbt**: строит витрину `analytics.orders_daily`
- **Оркестрация**: DAG `data_flow_mvp` в Airflow
- **Kafka-поток**: DAG `kafka_stream_mvp` (producer -> consumer -> ClickHouse -> dbt)

## Запуск

Скопируй переменные окружения (по желанию):

```bash
copy .env.example .env
```

Подними стек:

```bash
docker compose up -d --build
```

CDC-сервисы Debezium вынесены в отдельный профиль. Если сеть до контейнерного реестра доступна, их можно поднять отдельно:

```bash
docker compose --profile cdc up -d debezium debezium-init
```

Открой Airflow UI:
- `http://localhost:8088` (логин/пароль: `admin` / `admin`)

Запусти DAG `data_flow_mvp` вручную из UI.
Для Kafka-части запусти DAG `kafka_stream_mvp`.

## Доступы и интерфейсы

- **Airflow UI**: `http://localhost:8088`  
  Логин/пароль: `admin` / `admin`
- **MinIO Console**: `http://localhost:9001`  
  Логин/пароль: `minio` / `minio12345` (или из `.env`)
- **Spark Master UI**: `http://localhost:8080`
- **Spark Worker UI**: `http://localhost:8081`
- **Kafka UI**: `http://localhost:8085`
- **Jupyter (PySpark)**: `http://localhost:8889`  
  Токен: `dataflow` (или `JUPYTER_TOKEN` из `.env`)
- **ClickHouse HTTP**: `http://localhost:8123/ping`  
  Логин/пароль: `analytics` / `analytics`
- **Kafka Bootstrap**:
  - из контейнеров: `kafka:9092`
  - с хоста: `localhost:29092`
- **Postgres (source)**: `localhost:5434`  
  БД/логин/пароль: `source` / `source` / `source`
- **Postgres (airflow metadata)**: `localhost:5433`  
  БД/логин/пароль: `airflow` / `airflow` / `airflow`

## Полезные порты

- **Airflow**: `8088`
- **Spark UI (master)**: `8080`
- **MinIO**: `9000` (S3), `9001` (console)
- **Kafka**: `9092`
- **Kafka (host listener)**: `29092`
- **Kafka UI**: `8085`
- **Jupyter**: `8889`
- **ClickHouse**: `8123` (HTTP)
- **Postgres Airflow**: `5433`
- **Postgres Source**: `5434`
