CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS weather_observations_raw (
    event_time TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    message_id TEXT NOT NULL,
    source TEXT NOT NULL,
    continent TEXT NOT NULL,
    country TEXT NOT NULL,
    region TEXT NOT NULL,
    city TEXT NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    humidity DOUBLE PRECISION NOT NULL,
    wind_speed DOUBLE PRECISION NOT NULL,
    payload_json JSONB NOT NULL,
    PRIMARY KEY (message_id)
);

SELECT create_hypertable('weather_observations_raw', 'event_time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_weather_observations_city_time
    ON weather_observations_raw (city, event_time DESC);

CREATE TABLE IF NOT EXISTS weather_city_minute_aggregates (
    bucket_start TIMESTAMPTZ NOT NULL,
    continent TEXT NOT NULL,
    country TEXT NOT NULL,
    region TEXT NOT NULL,
    city TEXT NOT NULL,
    observation_count BIGINT NOT NULL,
    min_temperature DOUBLE PRECISION NOT NULL,
    max_temperature DOUBLE PRECISION NOT NULL,
    avg_temperature DOUBLE PRECISION NOT NULL,
    avg_humidity DOUBLE PRECISION NOT NULL,
    avg_wind_speed DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (bucket_start, continent, country, region, city)
);

SELECT create_hypertable('weather_city_minute_aggregates', 'bucket_start', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_weather_aggregate_city_bucket
    ON weather_city_minute_aggregates (city, bucket_start DESC);
