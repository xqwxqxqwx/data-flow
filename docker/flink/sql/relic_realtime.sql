-- Relic Route real-time layer (phase 3).
--   cdc.relic.* (Debezium Avro changelog) -> Flink -> rt.relic.* (upsert Avro) -> ClickHouse
-- Results are pure functions of the CDC data (no processing-time logic), so replaying the
-- CDC topics from the beginning always rebuilds the same state.

SET 'execution.runtime-mode' = 'streaming';
SET 'pipeline.name' = 'relic-realtime';
-- Postgres TIMESTAMPs are UTC; keep Flink's time conversions in UTC too
SET 'table.local-time-zone' = 'UTC';

-- ---------------------------------------------------------------------------
-- Sources: Debezium CDC. Timestamps arrive as epoch micros (io.debezium.time.MicroTimestamp),
-- dates as days since epoch (io.debezium.time.Date).
-- ---------------------------------------------------------------------------

CREATE TABLE items_cdc (
  item_id     BIGINT,
  title       STRING,
  category    STRING,
  brand       STRING,
  grade       STRING,
  status      STRING,
  list_price  DECIMAL(12, 2),
  listed_at   BIGINT,
  updated_at  BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'cdc.relic.items',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-relic-realtime-items',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-avro-confluent',
  'debezium-avro-confluent.url' = 'http://schema-registry:8081'
);

CREATE TABLE orders_cdc (
  order_id      BIGINT,
  customer_id   BIGINT,
  order_ts      BIGINT,
  channel       STRING,
  status        STRING,
  total_amount  DECIMAL(12, 2),
  van_id        BIGINT,
  stop_id       BIGINT,
  updated_at    BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'cdc.relic.orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-relic-realtime-orders',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-avro-confluent',
  'debezium-avro-confluent.url' = 'http://schema-registry:8081'
);

CREATE TABLE van_stops_cdc (
  stop_id      BIGINT,
  van_id       BIGINT,
  location_id  BIGINT,
  stop_date    INT,
  started_at   BIGINT,
  ended_at     BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'cdc.relic.van_stops',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-relic-realtime-van_stops',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-avro-confluent',
  'debezium-avro-confluent.url' = 'http://schema-registry:8081'
);

CREATE TABLE vans_cdc (
  van_id    BIGINT,
  name      STRING,
  van_type  STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'cdc.relic.vans',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-relic-realtime-vans',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-avro-confluent',
  'debezium-avro-confluent.url' = 'http://schema-registry:8081'
);

CREATE TABLE stop_locations_cdc (
  location_id  BIGINT,
  name         STRING,
  district     STRING,
  lat          DECIMAL(9, 6),
  lon          DECIMAL(9, 6)
) WITH (
  'connector' = 'kafka',
  'topic' = 'cdc.relic.stop_locations',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-relic-realtime-stop_locations',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-avro-confluent',
  'debezium-avro-confluent.url' = 'http://schema-registry:8081'
);

-- ---------------------------------------------------------------------------
-- Sinks: upsert topics (latest row per key; Avro schemas in the registry).
-- ---------------------------------------------------------------------------

CREATE TABLE rt_inventory (
  item_id     BIGINT NOT NULL,
  title       STRING,
  category    STRING,
  grade       STRING,
  status      STRING,
  list_price  DECIMAL(12, 2),
  listed_at   TIMESTAMP(3),
  updated_at  TIMESTAMP(3),
  PRIMARY KEY (item_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'rt.relic.inventory',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'avro-confluent',
  'key.avro-confluent.url' = 'http://schema-registry:8081',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://schema-registry:8081'
);

CREATE TABLE rt_inventory_by_category (
  category       STRING NOT NULL,
  listed_cnt     BIGINT,
  reserved_cnt   BIGINT,
  sold_cnt       BIGINT,
  listed_value   DECIMAL(14, 2),
  PRIMARY KEY (category) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'rt.relic.inventory_by_category',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'avro-confluent',
  'key.avro-confluent.url' = 'http://schema-registry:8081',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://schema-registry:8081'
);

CREATE TABLE rt_van_stops_live (
  stop_id        BIGINT NOT NULL,
  stop_date      DATE,
  van_id         BIGINT,
  van_name       STRING,
  van_type       STRING,
  location_id    BIGINT,
  location_name  STRING,
  district       STRING,
  lat            DECIMAL(9, 6),
  lon            DECIMAL(9, 6),
  started_at     TIMESTAMP(3),
  ended_at       TIMESTAMP(3),
  orders_cnt     BIGINT,
  gmv            DECIMAL(14, 2),
  last_sale_at   TIMESTAMP(3),
  PRIMARY KEY (stop_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'rt.relic.van_stops_live',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'avro-confluent',
  'key.avro-confluent.url' = 'http://schema-registry:8081',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://schema-registry:8081'
);

-- ---------------------------------------------------------------------------
-- Pipelines (one job)
-- ---------------------------------------------------------------------------

EXECUTE STATEMENT SET
BEGIN

-- R1: live status of every unique piece
INSERT INTO rt_inventory
SELECT
  item_id,
  title,
  category,
  grade,
  status,
  list_price,
  CAST(TO_TIMESTAMP_LTZ(listed_at / 1000, 3) AS TIMESTAMP(3)),
  CAST(TO_TIMESTAMP_LTZ(updated_at / 1000, 3) AS TIMESTAMP(3))
FROM items_cdc;

-- R1: stock per category
INSERT INTO rt_inventory_by_category
SELECT
  category,
  SUM(CASE WHEN status = 'listed' THEN 1 ELSE 0 END),
  SUM(CASE WHEN status = 'reserved' THEN 1 ELSE 0 END),
  SUM(CASE WHEN status = 'sold' THEN 1 ELSE 0 END),
  CAST(SUM(CASE WHEN status = 'listed' THEN list_price ELSE 0 END) AS DECIMAL(14, 2))
FROM items_cdc
GROUP BY category;

-- R2: every van stop with its running takings
INSERT INTO rt_van_stops_live
SELECT
  s.stop_id,
  CAST(TO_TIMESTAMP_LTZ(CAST(s.stop_date AS BIGINT) * 86400000, 3) AS DATE),
  s.van_id,
  v.name,
  v.van_type,
  s.location_id,
  l.name,
  l.district,
  l.lat,
  l.lon,
  CAST(TO_TIMESTAMP_LTZ(s.started_at / 1000, 3) AS TIMESTAMP(3)),
  CAST(TO_TIMESTAMP_LTZ(s.ended_at / 1000, 3) AS TIMESTAMP(3)),
  COALESCE(o.orders_cnt, 0),
  CAST(COALESCE(o.gmv, 0) AS DECIMAL(14, 2)),
  CAST(TO_TIMESTAMP_LTZ(o.last_sale_us / 1000, 3) AS TIMESTAMP(3))
FROM van_stops_cdc AS s
JOIN vans_cdc AS v ON s.van_id = v.van_id
JOIN stop_locations_cdc AS l ON s.location_id = l.location_id
LEFT JOIN (
  SELECT
    stop_id,
    COUNT(*) AS orders_cnt,
    SUM(total_amount) AS gmv,
    MAX(order_ts) AS last_sale_us
  FROM orders_cdc
  WHERE channel = 'van' AND status <> 'cancelled'
  GROUP BY stop_id
) AS o ON s.stop_id = o.stop_id;

END;
