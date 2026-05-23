-- Hardware Telemetry TimescaleDB schema

-- Raw telemetry: preserves every sample
CREATE TABLE IF NOT EXISTS hardware_telemetry_raw (
    message_id TEXT PRIMARY KEY,
    source TEXT,
    node_id TEXT,
    asset_tag TEXT,
    hostname TEXT,
    site_id TEXT,
    rack_id TEXT,
    region TEXT,
    geo_region TEXT,
    event_time TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    sample_seq BIGINT,

    -- common hardware metrics
    cpu_temp_c DOUBLE PRECISION,
    cpu_util_pct DOUBLE PRECISION,
    gpu_temp_c DOUBLE PRECISION,
    gpu_util_pct DOUBLE PRECISION,
    vram_used_mb DOUBLE PRECISION,
    vram_total_mb DOUBLE PRECISION,
    mem_used_mb DOUBLE PRECISION,
    mem_total_mb DOUBLE PRECISION,
    mem_pressure_pct DOUBLE PRECISION,
    power_draw_w DOUBLE PRECISION,
    fan_rpm DOUBLE PRECISION,
    thermal_throttle_active BOOLEAN,
    thermal_throttle_reason TEXT,
    disk_util_pct DOUBLE PRECISION,
    nvme_temp_c DOUBLE PRECISION,
    net_tx_mbps DOUBLE PRECISION,
    net_rx_mbps DOUBLE PRECISION,
    net_latency_ms DOUBLE PRECISION,
    packet_loss_pct DOUBLE PRECISION,
    ecc_error_count BIGINT,
    pcie_error_count BIGINT,
    health_score DOUBLE PRECISION,
    status_flags TEXT,
    source_vendor TEXT,
    source_model TEXT,

    payload_json JSONB
);

-- Convert to hypertable on event_time for time-series performance
SELECT create_hypertable('hardware_telemetry_raw', 'event_time', if_not_exists => TRUE);

-- Indexes commonly used for queries
CREATE INDEX IF NOT EXISTS idx_hwtelemetry_raw_node_time ON hardware_telemetry_raw (node_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_hwtelemetry_raw_site_time ON hardware_telemetry_raw (site_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_hwtelemetry_raw_region_time ON hardware_telemetry_raw (region, event_time DESC);

-- Rollup table: per-node minute aggregates for dashboards
CREATE TABLE IF NOT EXISTS node_minute_aggregates (
    bucket_start TIMESTAMPTZ NOT NULL,
    node_id TEXT NOT NULL,
    site_id TEXT,
    region TEXT,
    geo_region TEXT,
    sample_count BIGINT,
    min_cpu_temp_c DOUBLE PRECISION,
    max_cpu_temp_c DOUBLE PRECISION,
    avg_cpu_temp_c DOUBLE PRECISION,
    avg_cpu_util_pct DOUBLE PRECISION,
    avg_gpu_util_pct DOUBLE PRECISION,
    avg_vram_used_pct DOUBLE PRECISION,
    avg_mem_pressure_pct DOUBLE PRECISION,
    avg_power_draw_w DOUBLE PRECISION,
    avg_fan_rpm DOUBLE PRECISION,
    throttle_count BIGINT,
    alert_count BIGINT,
    health_score_avg DOUBLE PRECISION,
    health_score_min DOUBLE PRECISION,
    PRIMARY KEY (bucket_start, node_id)
);

SELECT create_hypertable('node_minute_aggregates', 'bucket_start', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_node_minute_site_bucket ON node_minute_aggregates (site_id, bucket_start DESC);

-- Incident table: track hardware incidents
CREATE TABLE IF NOT EXISTS hardware_incidents (
    incident_id SERIAL PRIMARY KEY,
    incident_time TIMESTAMPTZ NOT NULL,
    node_id TEXT,
    site_id TEXT,
    region TEXT,
    incident_type TEXT,
    severity TEXT,
    state TEXT,
    summary TEXT,
    details_json JSONB,
    opened_at TIMESTAMPTZ DEFAULT now(),
    closed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_incidents_node_time ON hardware_incidents (node_id, incident_time DESC);

-- Inventory table: static node metadata
CREATE TABLE IF NOT EXISTS node_inventory (
    node_id TEXT PRIMARY KEY,
    asset_tag TEXT,
    hostname TEXT,
    site_id TEXT,
    rack_id TEXT,
    vendor TEXT,
    model TEXT,
    cpu_model TEXT,
    gpu_model TEXT,
    ram_gb DOUBLE PRECISION,
    os_version TEXT,
    bios_version TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    retired_at TIMESTAMPTZ
);

-- Optional: maintenance helpers

-- Example insert functions or views can be added later as needed.
