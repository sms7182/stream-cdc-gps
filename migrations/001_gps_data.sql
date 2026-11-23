-- GPS points written by the Kafka Connect JDBC sink (topic gps-data).
-- geo_point is computed by Postgres from latitude/longitude on every insert,
-- so no CDC/Debezium step is needed to build the geometry.
--
-- Run once against trafficdb:
--   psql -h <host> -U postgres -d trafficdb -f migrations/001_gps_data.sql

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS gps_data (
    pk          bigserial PRIMARY KEY,
    id          text             NOT NULL, -- device timestamp sent in location.timestamp
    device_id   text             NOT NULL,
    heading     double precision,
    speed       double precision,
    accuracy    double precision,
    altitude    double precision,
    latitude    double precision NOT NULL,
    longitude   double precision NOT NULL,
    received_at timestamptz      NOT NULL DEFAULT now(),
    geo_point   geometry(Point, 4326)
        GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)) STORED
);

CREATE INDEX IF NOT EXISTS gps_data_geo_point_idx ON gps_data USING GIST (geo_point);
CREATE INDEX IF NOT EXISTS gps_data_device_idx    ON gps_data (device_id, received_at);
