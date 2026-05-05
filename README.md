# data-flow

Пет-проект по data engineering в Docker: **Airflow + Kafka + Spark + MinIO(S3) + Iceberg + ClickHouse + dbt + Prometheus + Grafana**.

## Архитектура слоев (layered architecture)

- **Bronze**
  - сырые события из Kafka сохраняются в MinIO (`raw/bronze/...`)
  - сырые данные в ClickHouse: `analytics.bronze_orders_stream`
- **Silver**
  - Spark чистит и дедуплицирует поток в Iceberg: `local.silver.orders_stream`
  - dbt-модель: `silver_orders_stream`
- **Gold**
  - витрина в dbt: `gold_orders_daily`
  - готова для BI/дашбордов

## End-to-end DAG (prod-like)

Новый DAG: `prod_like_e2e_pipeline`

Пайплайн:
1. Producer публикует события в Kafka (`orders_events_prod`)
2. Consumer сохраняет raw JSONL в MinIO (bronze)
3. Spark job `bronze_orders_to_iceberg_silver.py` строит silver-таблицу в Iceberg
4. Данные попадают в ClickHouse (`bronze_orders_stream`)
5. dbt запускает layered-модели: bronze -> silver -> gold
6. dbt tests валидируют качество данных

## dbt tests

В проекте есть:
- generic tests (`not_null`, `unique`, `accepted_values`)
- singular test: `dbt/tests/silver_orders_amount_non_negative.sql`

Ручной запуск внутри контейнера `airflow`:

```bash
docker compose exec airflow bash -lc "cd /opt/dbt && /opt/dbt_venv/bin/dbt run --profiles-dir /opt/dbt"
docker compose exec airflow bash -lc "cd /opt/dbt && /opt/dbt_venv/bin/dbt test --profiles-dir /opt/dbt"
```

## Observability: Prometheus + Grafana

Добавлены сервисы:
- `prometheus` (`http://localhost:9090`)
- `grafana` (`http://localhost:3000`, `admin/admin`)
- `statsd-exporter` (Airflow metrics -> Prometheus)
- `kafka-exporter`
- `clickhouse-exporter`

### Куда смотреть в Prometheus

- `http://localhost:9090/targets`  
  Проверяй, что все targets в `UP`.
- Примеры полезных запросов:
  - `airflow_scheduler_heartbeat`
  - `sum(rate(airflow_ti_failures[5m]))`
  - `topk(5, kafka_consumergroup_lag)`
  - `clickhouse_up`

### Куда смотреть в Grafana

- Открой `Dashboards -> Data Flow -> Data Flow Overview`
- Базовые панели:
  - Airflow scheduler heartbeat
  - Kafka consumer lag
  - ClickHouse availability
  - Airflow task failures rate

Если панель пустая, сначала проверь `Prometheus targets`, потом запусти любой DAG и подожди 1-2 минуты.

## Distributed Spark + Flink streaming

Теперь в проекте:
- **Spark distributed**: `spark-master` + `spark-worker` (3 cores) + `spark-worker-2` (2 cores)
- **Flink streaming**: `flink-jobmanager` + `flink-taskmanager` + `flink-sql-runner`
- **Always-on producer**: сервис `kafka-orders-producer` постоянно пишет в Kafka topic `orders_events_stream`
- **Streaming flow без Airflow-consumer**:
  - `orders_events_stream` -> Flink SQL enrichment -> `orders_events_enriched`
  - ClickHouse Kafka Engine + Materialized View читают `orders_events_enriched` автоматически в `analytics.orders_events_enriched`

Flink UI:
- `http://localhost:8087`

## Запуск

```bash
copy .env.example .env
docker compose up -d --build
```

Debezium CDC поднимается автоматически вместе со всем стеком (`debezium` + `debezium-init`).

Открой Airflow UI:
- `http://localhost:8088` (`admin/admin`)

Рекомендуемый порядок для проверки:
1. `kafka_stream_mvp`
2. `data_flow_mvp`
3. `cdc_orders_mvp`
4. `prod_like_e2e_pipeline`
5. Проверить always-on streaming (`Flink + ClickHouse`) без запуска DAG

## Интерфейсы

- Airflow: `http://localhost:8088`
- Grafana: `http://localhost:3000`
- Prometheus: `http://localhost:9090`
- Kafka UI: `http://localhost:8085`
- Flink UI: `http://localhost:8087`
- Spark Master UI: `http://localhost:8080`
- Spark Worker UI: `http://localhost:8081`
- MinIO Console: `http://localhost:9001`
- Jupyter: `http://localhost:8889`
- ClickHouse ping: `http://localhost:8123/ping`
