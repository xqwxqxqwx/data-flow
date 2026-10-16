-- Relic Route real-time serving layer (phase 3).
-- Flink upsert topics rt.relic.* (Avro + Schema Registry) -> Kafka engine -> MV -> ReplacingMergeTree.
-- Version column = Kafka offset: one key always lands in one partition, so a later offset is a later
-- state. Read with FINAL (or the v_* views) to get exactly one row per key.
-- Upsert tombstones (empty value) and unparsable messages surface in _error and are skipped.

-- ---------------------------------------------------------------------------
-- Live inventory: one row per unique piece
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.rt_relic_inventory_queue
(
  item_id Int64,
  title Nullable(String),
  category Nullable(String),
  grade Nullable(String),
  status Nullable(String),
  list_price Nullable(Decimal(12, 2)),
  listed_at Nullable(DateTime64(3, 'UTC')),
  updated_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'rt.relic.inventory',
  kafka_group_name = 'clickhouse-rt-relic-inventory',
  kafka_format = 'AvroConfluent',
  format_avro_schema_registry_url = 'http://schema-registry:8081',
  kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS analytics.rt_relic_inventory
(
  item_id UInt64,
  title String,
  category LowCardinality(String),
  grade LowCardinality(String),
  status LowCardinality(String),
  list_price Decimal(12, 2),
  listed_at Nullable(DateTime64(3, 'UTC')),
  updated_at DateTime64(3, 'UTC'),
  kafka_offset UInt64
)
ENGINE = ReplacingMergeTree(kafka_offset)
ORDER BY item_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_rt_relic_inventory
TO analytics.rt_relic_inventory
AS
SELECT
  toUInt64(item_id) AS item_id,
  ifNull(title, '') AS title,
  ifNull(category, '') AS category,
  ifNull(grade, '') AS grade,
  ifNull(status, '') AS status,
  ifNull(list_price, 0) AS list_price,
  listed_at,
  ifNull(updated_at, toDateTime64(0, 3, 'UTC')) AS updated_at,
  _offset AS kafka_offset
FROM analytics.rt_relic_inventory_queue
WHERE length(_error) = 0;

-- ---------------------------------------------------------------------------
-- Stock per category
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.rt_relic_inventory_by_category_queue
(
  category String,
  listed_cnt Nullable(Int64),
  reserved_cnt Nullable(Int64),
  sold_cnt Nullable(Int64),
  listed_value Nullable(Decimal(14, 2))
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'rt.relic.inventory_by_category',
  kafka_group_name = 'clickhouse-rt-relic-inventory-by-category',
  kafka_format = 'AvroConfluent',
  format_avro_schema_registry_url = 'http://schema-registry:8081',
  kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS analytics.rt_relic_inventory_by_category
(
  category LowCardinality(String),
  listed_cnt UInt64,
  reserved_cnt UInt64,
  sold_cnt UInt64,
  listed_value Decimal(14, 2),
  kafka_offset UInt64
)
ENGINE = ReplacingMergeTree(kafka_offset)
ORDER BY category;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_rt_relic_inventory_by_category
TO analytics.rt_relic_inventory_by_category
AS
SELECT
  category,
  toUInt64(ifNull(listed_cnt, 0)) AS listed_cnt,
  toUInt64(ifNull(reserved_cnt, 0)) AS reserved_cnt,
  toUInt64(ifNull(sold_cnt, 0)) AS sold_cnt,
  ifNull(listed_value, 0) AS listed_value,
  _offset AS kafka_offset
FROM analytics.rt_relic_inventory_by_category_queue
WHERE length(_error) = 0;

-- ---------------------------------------------------------------------------
-- Van stops with running takings
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.rt_relic_van_stops_live_queue
(
  stop_id Int64,
  stop_date Nullable(Date),
  van_id Nullable(Int64),
  van_name Nullable(String),
  van_type Nullable(String),
  location_id Nullable(Int64),
  location_name Nullable(String),
  district Nullable(String),
  lat Nullable(Decimal(9, 6)),
  lon Nullable(Decimal(9, 6)),
  started_at Nullable(DateTime64(3, 'UTC')),
  ended_at Nullable(DateTime64(3, 'UTC')),
  orders_cnt Nullable(Int64),
  gmv Nullable(Decimal(14, 2)),
  last_sale_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'rt.relic.van_stops_live',
  kafka_group_name = 'clickhouse-rt-relic-van-stops-live',
  kafka_format = 'AvroConfluent',
  format_avro_schema_registry_url = 'http://schema-registry:8081',
  kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS analytics.rt_relic_van_stops_live
(
  stop_id UInt64,
  stop_date Date,
  van_id UInt64,
  van_name String,
  van_type LowCardinality(String),
  location_id UInt64,
  location_name String,
  district LowCardinality(String),
  lat Nullable(Decimal(9, 6)),
  lon Nullable(Decimal(9, 6)),
  started_at DateTime64(3, 'UTC'),
  ended_at Nullable(DateTime64(3, 'UTC')),
  orders_cnt UInt64,
  gmv Decimal(14, 2),
  last_sale_at Nullable(DateTime64(3, 'UTC')),
  kafka_offset UInt64
)
ENGINE = ReplacingMergeTree(kafka_offset)
ORDER BY stop_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_rt_relic_van_stops_live
TO analytics.rt_relic_van_stops_live
AS
SELECT
  toUInt64(stop_id) AS stop_id,
  ifNull(stop_date, toDate(0)) AS stop_date,
  toUInt64(ifNull(van_id, 0)) AS van_id,
  ifNull(van_name, '') AS van_name,
  ifNull(van_type, '') AS van_type,
  toUInt64(ifNull(location_id, 0)) AS location_id,
  ifNull(location_name, '') AS location_name,
  ifNull(district, '') AS district,
  lat,
  lon,
  ifNull(started_at, toDateTime64(0, 3, 'UTC')) AS started_at,
  ended_at,
  toUInt64(ifNull(orders_cnt, 0)) AS orders_cnt,
  ifNull(gmv, 0) AS gmv,
  last_sale_at,
  _offset AS kafka_offset
FROM analytics.rt_relic_van_stops_live_queue
WHERE length(_error) = 0;

-- ---------------------------------------------------------------------------
-- Read side
-- ---------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS analytics.v_relic_live_inventory AS
SELECT * EXCEPT kafka_offset
FROM analytics.rt_relic_inventory FINAL;

CREATE VIEW IF NOT EXISTS analytics.v_relic_live_inventory_by_category AS
SELECT * EXCEPT kafka_offset
FROM analytics.rt_relic_inventory_by_category FINAL;

-- vans working today (Moscow date), open first
CREATE VIEW IF NOT EXISTS analytics.v_relic_live_van_today AS
SELECT
  van_name,
  van_type,
  location_name,
  district,
  lat,
  lon,
  toTimeZone(started_at, 'Europe/Moscow') AS started_at_msk,
  toTimeZone(ended_at, 'Europe/Moscow') AS ended_at_msk,
  isNull(ended_at) AND started_at <= now64(3) AS is_open,
  orders_cnt,
  gmv,
  toTimeZone(last_sale_at, 'Europe/Moscow') AS last_sale_at_msk
FROM analytics.rt_relic_van_stops_live FINAL
WHERE stop_date = toDate(now('Europe/Moscow'))
ORDER BY is_open DESC, gmv DESC;
