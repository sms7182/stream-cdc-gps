-- ClickHouse reads the same Kafka topic (gps-data) as the Postgres JDBC sink,
-- in its own consumer group, so the two sinks are independent.
--
--   Kafka gps-data -> gps.gps_data_queue (Kafka engine)
--                  -> gps.gps_data_mv (materialized view, parses the JSON)
--                  -> gps.gps_points (MergeTree, the table you query)
--
-- The official image runs this file automatically on first start (empty data
-- volume). To apply it to an existing server:
--   docker compose exec -T clickhouse clickhouse-client --multiquery < clickhouse/init/001_gps.sql

CREATE DATABASE IF NOT EXISTS gps;

-- Final storage. One row per GPS point.
CREATE TABLE IF NOT EXISTS gps.gps_points
(
    device_id       LowCardinality(String),
    device_ts       String,                     -- timestamp exactly as the device sent it
    event_time      DateTime64(3, 'UTC'),       -- parsed device_ts, or arrival time if unparsable
    latitude        Float64,
    longitude       Float64,
    location        Point,                      -- (longitude, latitude), for geo functions
    heading         Float64,
    speed           Float64,
    accuracy        Float64,
    altitude        Float64,
    kafka_partition UInt32,
    kafka_offset    UInt64,
    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (device_id, event_time);

-- Kafka consumer. Each message is a Kafka Connect envelope {"schema":..,"payload":..},
-- so it is read as one JSON string and unpacked in the materialized view.
CREATE TABLE IF NOT EXISTS gps.gps_data_queue
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'gps-data',
    kafka_group_name = 'clickhouse-gps',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 100;

-- Moves every batch from the queue into gps_points.
CREATE MATERIALIZED VIEW IF NOT EXISTS gps.gps_data_mv TO gps.gps_points AS
SELECT
    JSONExtractString(raw, 'payload', 'device_id') AS device_id,
    JSONExtractString(raw, 'payload', 'id')        AS device_ts,
    ifNull(parseDateTime64BestEffortOrNull(device_ts, 3, 'UTC'), now64(3, 'UTC')) AS event_time,
    JSONExtractFloat(raw, 'payload', 'latitude')   AS latitude,
    JSONExtractFloat(raw, 'payload', 'longitude')  AS longitude,
    (longitude, latitude)                          AS location,
    JSONExtractFloat(raw, 'payload', 'heading')    AS heading,
    JSONExtractFloat(raw, 'payload', 'speed')      AS speed,
    JSONExtractFloat(raw, 'payload', 'accuracy')   AS accuracy,
    JSONExtractFloat(raw, 'payload', 'altitude')   AS altitude,
    _partition                                     AS kafka_partition,
    _offset                                        AS kafka_offset
FROM gps.gps_data_queue
WHERE device_id != '';
