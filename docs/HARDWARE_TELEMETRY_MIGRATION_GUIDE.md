# Hardware Telemetry Migration Guide

This guide explains how to convert the current Weather RTOS codebase into a worldwide hardware telemetry system. It is written as a practical handoff document that can be reused in a new repository chat or implementation branch.

The main idea is simple:
- keep the existing hierarchical architecture
- replace the weather domain with hardware fleet telemetry
- preserve the transport, aggregation, and TimescaleDB writer patterns
- expand the simulator, schema, and validation logic for CPU, GPU, memory, power, thermal, and network telemetry

## 1. Executive Summary

The current repository already contains the right structural pieces for a global telemetry platform:
- collectors that generate data
- hierarchical aggregators that validate and roll up data
- a bounded writer that persists to TimescaleDB
- a simulator for synthetic traffic
- shared protocol, transport, and queueing helpers

What changes is the domain model:
- weather packets become hardware telemetry packets
- city and region concepts become node, rack, site, region, and global fleet concepts
- weather tables become raw telemetry, rollup, incident, and inventory tables
- weather simulation becomes hardware load and failure simulation

The result is a distributed hardware observability platform for fleets of servers, GPUs, edge boxes, lab machines, or embedded nodes.

## 2. Target Use Case

The best target use case is a worldwide hardware telemetry system that continuously streams:
- CPU temperature
- CPU utilization
- GPU temperature
- GPU utilization
- VRAM usage
- memory pressure
- power draw
- thermal throttling state
- fan RPM
- disk utilization
- NVMe temperature
- NIC throughput
- network latency
- packet loss
- ECC or PCIe error counts
- health score and alert state

This architecture fits especially well when:
- nodes are distributed across many sites
- telemetry is high-frequency
- local buffering is required during outages
- vendor-specific sensor formats must be normalized
- regional and global rollups are needed for alerting and trend analysis

## 3. What to Keep From the Current Repository

The current repository already has the important design patterns.
Keep these ideas intact:
- hierarchical topology-driven routing
- shared message envelope
- validation before aggregation
- bounded async queueing before DB writes
- in-process broker mode for local testing
- TCP transport for distributed mode
- simulator as a first-class component
- raw plus aggregate persistence in TimescaleDB

These files are especially relevant:
- `collectors/regional/main.cpp`
- `aggregator/hierarchical/main.cpp`
- `timescale_writer/main.cpp`
- `simulator/main.cpp`
- `common/models/WeatherPacket.hpp`
- `common/protocol/MessageTypes.hpp`
- `common/pipeline/ValidationAggregationConsumerPipeline.hpp`
- `common/timescale/AsyncQueueWriter.hpp`
- `common/timescale/TimescaleBatchWriter.hpp`
- `timescale/schema.sql`
- `demo.sh`

You should not rebuild the architecture from scratch. You should re-theme and extend the existing one.

## 4. Domain Mapping

The weather terminology maps cleanly to hardware telemetry terminology.

### Current weather concept -> New hardware concept
- `city` -> `node` or `device`
- `region` -> `site` or `rack`
- `continent` -> `geo_region` or `super_region`
- `weather collector` -> `edge agent` or `node collector`
- `weather aggregator` -> `site aggregator` or `fleet aggregator`
- `WeatherPacket` -> `TelemetryPacket`
- `weather observations raw` -> `hardware_telemetry_raw`
- `weather city minute aggregates` -> `node_minute_aggregates`
- weather alerts -> thermal / hardware incidents

### Suggested hierarchy
- node level: individual device or host
- rack level: group of machines in a rack
- site level: one datacenter, lab, building, or industrial site
- region level: a geographic cluster of sites
- global level: fleet-wide consolidation

This hierarchy should remain topology-driven so the same software can scale from a lab of 10 nodes to a fleet of thousands.

## 5. Recommended System Architecture

### 5.1 Edge node agent

Each monitored device runs an agent that samples local telemetry.

Responsibilities:
- read hardware sensors
- normalize units
- attach node identity
- buffer locally during network issues
- emit periodic heartbeats
- send raw telemetry samples
- send alerts when thresholds are crossed

Possible data sources on Linux:
- `/proc`
- `/sys`
- `lm-sensors`
- `nvidia-smi`
- DCGM exporter or direct DCGM access
- ROCm SMI
- IPMI and Redfish
- `iostat`, `vmstat`, and network interface statistics

### 5.2 Site collector

A site collector receives telemetry from many nodes in the same site.

Responsibilities:
- validate basic ranges
- deduplicate samples
- tag site and rack metadata
- compute short-window health summaries
- suppress noisy duplicate events
- forward upstream only what is needed

### 5.3 Regional aggregator

A regional aggregator combines data from many sites.

Responsibilities:
- compute fleet-level trends across sites
- compare nodes against peer cohorts
- detect regional anomalies
- aggregate alert counts
- forward summaries to the global sink

### 5.4 Global aggregator

The global aggregator produces fleet-wide summaries.

Responsibilities:
- compute global health scores
- identify widespread incidents
- correlate issues across regions
- feed long-term storage and dashboards

### 5.5 Timescale writer

The writer persists normalized samples, rollups, and incidents.

Responsibilities:
- batch inserts
- preserve raw history
- write rollups for dashboards
- write incident records
- keep insert throughput high and predictable

### 5.6 Simulator

The simulator generates realistic telemetry traffic and failure modes.

Responsibilities:
- emulate healthy nodes
- emulate stressed nodes
- emulate thermal events
- emulate fan failures
- emulate GPU memory pressure
- emulate network loss and reconnects
- emulate sensor drift and bad readings
- emulate site outages and replay bursts

## 6. Core Data Model

The single most important code change is replacing the weather payload with a hardware telemetry payload.

### 6.1 Suggested TelemetryPacket fields

A good first version of `TelemetryPacket` should include:
- `node_id`
- `asset_tag`
- `hostname`
- `site_id`
- `rack_id`
- `region`
- `geo_region`
- `timestamp`
- `sample_seq`
- `cpu_temp_c`
- `cpu_util_pct`
- `gpu_temp_c`
- `gpu_util_pct`
- `vram_used_mb`
- `vram_total_mb`
- `mem_used_mb`
- `mem_total_mb`
- `mem_pressure_pct`
- `power_draw_w`
- `fan_rpm`
- `thermal_throttle_active`
- `thermal_throttle_reason`
- `disk_util_pct`
- `nvme_temp_c`
- `net_tx_mbps`
- `net_rx_mbps`
- `net_latency_ms`
- `packet_loss_pct`
- `ecc_error_count`
- `pcie_error_count`
- `health_score`
- `status_flags`
- `source_vendor`
- `source_model`
- `payload_json`

### 6.2 Why keep `payload_json`

Keep a generic JSON field for vendor-specific extensions because hardware telemetry is never fully uniform.

Examples of vendor-specific values:
- GPU clock speed
- CPU package power limits
- board temperature sensors
- battery health on edge devices
- fan zone temperature
- BMC sensor values
- custom error fields

Keeping a JSON extension field reduces schema churn and makes the system easier to evolve.

### 6.3 Validation rules

Validation should be strict enough to catch bad data but not so strict that it rejects all vendor variation.

Suggested checks:
- temperature values must be within realistic bounds unless explicitly flagged as invalid or sensor error
- utilization values must be between 0 and 100
- memory used cannot exceed memory total unless a special flag allows it
- fan RPM cannot be negative
- power draw must be positive
- timestamps should not drift too far into the future or past
- packet loss must be between 0 and 100
- health score should be clamped to the expected range

If the packet is invalid, do not silently drop it. Mark it with a reason code and route it to logs or incident handling.

## 7. Protocol and Message Types

The current protocol in `common/protocol/MessageTypes.hpp` should be expanded.

### 7.1 Suggested message types
- `telemetry.packet`
- `telemetry.heartbeat`
- `telemetry.alert`
- `telemetry.health.summary`
- `telemetry.inventory.update`
- `telemetry.diagnostic.dump`
- `telemetry.command`
- `telemetry.command.ack`
- `telemetry.topology.update`
- `canonical.message.envelope`

### 7.2 Why separate message types

Different message types should not be overloaded into one blob because they have different retention, routing, and alerting behavior.

Examples:
- a heartbeat should be small and frequent
- a telemetry packet should be regular and high volume
- an alert should be routed immediately
- an inventory update should be rare
- a diagnostic dump may be large and chunked
- a command needs acknowledgement and retry semantics

### 7.3 Envelope design

The canonical message envelope should remain the transport boundary.

It should hold:
- message ID
- source identity
- destination identity
- topic or routing key
- type
- timestamp
- correlation ID
- payload

The envelope should remain generic so the same transport layer works for future domains.

## 8. Database Design

The current Timescale schema is weather-specific and needs to be replaced with a hardware-oriented schema.

### 8.1 Raw telemetry table

Purpose:
- preserve every sample
- support forensic analysis
- support replay and backfill
- keep a full fidelity audit trail

Suggested table name:
- `hardware_telemetry_raw`

Suggested columns:
- `event_time`
- `created_at`
- `message_id`
- `node_id`
- `asset_tag`
- `hostname`
- `site_id`
- `rack_id`
- `region`
- `geo_region`
- `cpu_temp_c`
- `cpu_util_pct`
- `gpu_temp_c`
- `gpu_util_pct`
- `vram_used_mb`
- `vram_total_mb`
- `mem_used_mb`
- `mem_total_mb`
- `mem_pressure_pct`
- `power_draw_w`
- `fan_rpm`
- `thermal_throttle_active`
- `thermal_throttle_reason`
- `disk_util_pct`
- `nvme_temp_c`
- `net_tx_mbps`
- `net_rx_mbps`
- `net_latency_ms`
- `packet_loss_pct`
- `ecc_error_count`
- `pcie_error_count`
- `health_score`
- `status_flags`
- `source_vendor`
- `source_model`
- `payload_json`

Suggested indexes:
- hypertable on `event_time`
- primary key or unique key on `message_id`
- index on `(node_id, event_time DESC)`
- index on `(site_id, event_time DESC)`
- index on `(region, event_time DESC)`

### 8.2 Rollup table

Purpose:
- fast dashboard queries
- fleet summaries
- alert-friendly aggregates

Suggested table name:
- `node_minute_aggregates`

Suggested columns:
- `bucket_start`
- `node_id`
- `site_id`
- `region`
- `geo_region`
- `sample_count`
- `min_cpu_temp_c`
- `max_cpu_temp_c`
- `avg_cpu_temp_c`
- `avg_cpu_util_pct`
- `avg_gpu_util_pct`
- `avg_vram_used_pct`
- `avg_mem_pressure_pct`
- `avg_power_draw_w`
- `avg_fan_rpm`
- `throttle_count`
- `alert_count`
- `health_score_avg`
- `health_score_min`

You may later add site-level and region-level summary tables if needed.

### 8.3 Incident table

Purpose:
- track hardware incidents separately from regular samples
- support operational workflows
- support alert lifecycle management

Suggested table name:
- `hardware_incidents`

Suggested columns:
- `incident_time`
- `node_id`
- `site_id`
- `region`
- `incident_type`
- `severity`
- `state`
- `summary`
- `details_json`
- `opened_at`
- `closed_at`

Suggested incident types:
- `over_temp`
- `thermal_throttle`
- `gpu_failure`
- `memory_pressure`
- `fan_failure`
- `network_degradation`
- `power_spike`
- `sensor_fault`
- `node_offline`
- `clock_skew`
- `packet_drop_burst`

### 8.4 Inventory table

Optional but highly recommended.

Purpose:
- keep static hardware metadata out of every sample
- support asset lifecycle tracking
- support vendor and model grouping

Suggested table name:
- `node_inventory`

Suggested columns:
- `node_id`
- `asset_tag`
- `hostname`
- `site_id`
- `rack_id`
- `vendor`
- `model`
- `cpu_model`
- `gpu_model`
- `ram_gb`
- `os_version`
- `bios_version`
- `created_at`
- `retired_at`

## 9. Simulator Design

The simulator should become a fleet simulator, not just a generic traffic generator.

### 9.1 Responsibilities
- generate normal telemetry
- generate load spikes
- generate thermal ramp-up and cool-down behavior
- generate fan response behavior
- generate throttling events
- generate memory pressure events
- generate power spikes
- generate network degradation
- generate node offline and recovery events
- generate invalid or drifting sensor values
- generate vendor-specific variants

### 9.2 Recommended simulation modes
- `normal`
- `bursty`
- `failure-injection`
- `site-outage`
- `mixed-vendor`
- `replay`
- `load-test`

### 9.3 Example simulated scenarios
- idle node under nominal temperature
- CPU stress test with rising temperature
- GPU training workload with VRAM pressure
- fan slowdown causing throttling
- memory leak causing persistent pressure
- packet loss producing delayed delivery
- site outage causing backlog and replay
- clock skew creating out-of-order packets
- bad sensor readings caused by firmware bug
- power surge followed by cooling recovery

### 9.4 Simulator outputs
- raw telemetry packets
- heartbeats
- alerts
- diagnostic dumps
- optional topology updates

### 9.5 Simulator implementation advice

The simulator should produce realistic behavior, not just random numbers.
Use:
- seeded randomness for reproducibility
- stateful node models
- workload phases over time
- failure injection probabilities
- per-site environmental profiles
- per-vendor parameter distributions

This gives you repeatable tests and better demos.

## 10. Open Source Data Sources

Real hardware telemetry datasets are often limited, but there are many open-source tools and data sources that can feed or validate the system.

### 10.1 Useful open-source telemetry sources
- Prometheus `node_exporter`
- Telegraf
- OpenTelemetry Collector
- `lm-sensors`
- NVIDIA DCGM and `dcgm-exporter`
- AMD ROCm SMI
- IPMI tools
- Redfish endpoints
- collectd
- Fluent Bit for log-side integration

### 10.2 What to use them for
- live telemetry from your own lab
- schema validation
- testing unit conversions
- simulating vendor diversity
- comparing simulator output with real machine behavior
- building baseline ranges for temperatures, power, and utilization

### 10.3 Public or semi-public inputs to consider
- Prometheus sample metric exposition outputs
- node exporter scrape examples
- DCGM exporter outputs
- Redfish and IPMI sample responses
- benchmark telemetry from MLPerf-style workloads
- Kubernetes node metrics
- exported JSON or CSV from your own test hardware

### 10.4 Practical recommendation

Do not depend on one perfect public dataset.
Instead, combine:
- real telemetry from a small lab
- replayed snapshots
- open-source exporter outputs
- synthetic simulation

That gives you a much better platform for development and regression testing.

## 11. Collector Design Changes

The current collector logic should be repurposed from weather polling to hardware sampling.

### 11.1 New collector responsibilities
- sample local sensors
- normalize units
- attach node identity
- deduplicate repetitive values if needed
- buffer locally when the broker is unavailable
- publish telemetry packets upstream

### 11.2 Useful sampling sources by platform
On Linux:
- `/proc/stat`
- `/proc/meminfo`
- `/proc/net/dev`
- `/sys/class/thermal`
- `/sys/class/hwmon`
- `lm-sensors`
- GPU vendor tools
- IPMI or Redfish endpoints

### 11.3 Collector architecture advice
Keep the current worker-thread model because it fits sensor polling well.
A good pattern is:
- one polling thread per device class or sensor group
- one queue for outbound telemetry
- one sender thread
- bounded queue with backpressure

### 11.4 Collector metadata
Every sample should include:
- node identity
- site and rack metadata
- vendor and model
- sample sequence number
- timestamp source
- unit normalization status

## 12. Aggregator Design Changes

The current aggregator architecture is a strong fit for fleet telemetry.

### 12.1 New aggregator responsibilities
- validate packets
- deduplicate by message ID or sequence
- compute health summaries
- roll up per-node stats
- detect site-level anomalies
- route alerts upward
- forward only the necessary summaries to upper tiers

### 12.2 Aggregation metrics to compute
- average CPU temperature over a window
- maximum GPU temperature over a window
- average CPU utilization
- average GPU utilization
- 95th percentile power draw
- average fan RPM
- thermal throttle count
- memory pressure trend
- packet loss trend
- node uptime percentage
- percent of nodes in warning state
- percent of nodes in critical state

### 12.3 Validation rules at the aggregator
The aggregator should flag or reject:
- invalid ranges
- suspicious clock skew
- impossible temperature values
- impossible fan RPM values
- power draw anomalies outside expected hardware range
- malformed payloads
- duplicate message IDs

### 12.4 Tier-specific behavior
Site level:
- local validation
- fast alerting
- short-window rollups

Regional level:
- cross-site anomaly detection
- cohort comparison
- baseline drift detection

Global level:
- fleet health index
- trend analysis
- incident correlation across regions

## 13. Writer and Queueing Changes

The current writer design is already close to what you want.

### 13.1 Writer responsibilities
- accept validated telemetry from the pipeline
- batch inserts efficiently
- keep ordering stable where needed
- write raw samples
- write rollup records
- write incident records
- retain JSON payloads for future reprocessing

### 13.2 Queueing behavior
Keep the bounded queue.
Hardware telemetry can spike during:
- boot storms
- node restarts
- workload bursts
- thermal incidents
- reconnect floods after outages

A bounded queue with explicit backpressure is safer than an unbounded queue.

### 13.3 Database write strategy
Use a transaction or batch grouping where appropriate so that:
- raw sample and rollup entries stay consistent
- partial writes are minimized
- failure handling is clear

## 14. Configuration and Topology

The current topology config should be conceptually replaced with a hardware fleet topology file.

### 14.1 Suggested topology levels
- `node`
- `rack`
- `site`
- `region`
- `global_sink`

### 14.2 Suggested metadata per topology node
- name
- parent
- children
- vendor
- model
- role
- broker topic
- failure domain
- site climate profile
- threshold profile

### 14.3 Why topology matters
The same system should work for:
- a small lab of test machines
- a GPU cluster
- an edge fleet
- a globally distributed hardware fleet

Topology-driven configuration keeps the code generic and avoids hardcoding geography or hardware assumptions.

## 15. Observability Strategy

The telemetry system itself should be observable.

### 15.1 Internal metrics to export
- ingest rate
- queue depth
- queue overflow count
- invalid packet count
- deduplication count
- DB flush latency
- retry count
- alert count
- incident count
- health score distribution
- regional lag

### 15.2 Logging suggestions
Log:
- node registration
- collector startup and shutdown
- topology startup
- alert transitions
- queue pressure warnings
- DB write failures
- reconnect events
- invalid sensor values
- incident creation and resolution

### 15.3 Dashboard ideas
- hottest nodes by region
- GPU power spikes
- thermal throttling over time
- fan failure trends
- memory pressure by model
- network latency by site
- fleet health index
- incident timeline

## 16. Migration Plan

This is the safest order of operations.

### Phase 1: rename the domain model
- replace `WeatherPacket` with `TelemetryPacket`
- update message type strings
- update validation rules
- keep transport and concurrency intact

### Phase 2: update the simulator
- replace weather generation with telemetry generation
- add failure injection
- add stateful workload models
- add node/site diversity

### Phase 3: replace the database schema
- create raw telemetry table
- create rollup tables
- create incident table
- create inventory table if needed
- update writer SQL

### Phase 4: update collectors
- swap weather polling for hardware sampling
- add vendor adapters
- add local health scoring
- add buffering and reconnect logic

### Phase 5: update aggregators
- replace weather validation with hardware validation
- add anomaly detection
- compute rollups and fleet summaries
- route alerts and incidents

### Phase 6: harden the writer
- map telemetry payloads to schema columns
- preserve vendor JSON
- keep backpressure and batching
- add incident insert support

### Phase 7: add tests and load checks
- unit tests
- integration tests
- scenario tests
- performance tests
- replay tests

## 17. Testing Plan

You should test at several levels.

### 17.1 Unit tests
- packet parsing
- validation rules
- health score logic
- rollup math
- deduplication
- JSON extension parsing

### 17.2 Integration tests
- collector to aggregator
- aggregator to writer
- writer to TimescaleDB
- simulator to full pipeline
- reconnect after outage

### 17.3 Scenario tests
- overheated node
- fan failure
- GPU memory exhaustion
- network loss
- clock skew
- burst replay
- invalid sensor values
- site outage

### 17.4 Performance tests
- sustained telemetry rate
- burst traffic from many nodes
- queue saturation
- database write throughput
- recovery after backlog

### 17.5 Success criteria
- no deadlocks
- bounded queue behavior under load
- correct rollup counts
- correct alert generation
- correct incident tracking
- stable shutdown behavior

## 18. Retention and Compression

For TimescaleDB, think in terms of different retention classes.

### Raw telemetry
- short retention
- compressed after aging out of hot storage

### Rollups
- long retention
- used for dashboards and trend queries

### Incidents
- long retention
- valuable for operations and postmortems

### Inventory
- long retention
- changes infrequently

## 19. Recommended First Build Slice

If you want the fastest useful implementation path, do this first:
- rename the packet model
- update message types
- update the simulator to emit hardware telemetry
- replace the Timescale schema
- keep the transport and aggregator patterns unchanged for now

That gives you a working end-to-end hardware telemetry pipeline with minimal architectural churn.

## 20. Reuse Prompt for a New Repo Chat

Paste this into a new repo chat if you want implementation help:

Convert the existing hierarchical Weather RTOS codebase into a worldwide hardware telemetry system. Preserve the collector -> aggregator -> Timescale writer architecture, but replace the weather domain with node, rack, site, region, and global fleet telemetry. Rename WeatherPacket to TelemetryPacket, replace the weather schema with hardware raw, rollup, incident, and inventory tables, and update the simulator to generate realistic CPU, GPU, memory, power, fan, thermal, and network telemetry with failure injection. Keep in-process broker mode and TCP mode. Add support for vendor-specific payload extensions, node health scoring, alert routing, replay scenarios, and open-source data source adapters such as node_exporter, Telegraf, OpenTelemetry Collector, lm-sensors, DCGM, ROCm SMI, IPMI, and Redfish.

## 21. File References to Start With

- `README.md`
- `docs/ARCHITECTURE.md`
- `CMakeLists.txt`
- `common/models/WeatherPacket.hpp`
- `common/protocol/MessageTypes.hpp`
- `common/pipeline/ValidationAggregationConsumerPipeline.hpp`
- `common/timescale/AsyncQueueWriter.hpp`
- `common/timescale/TimescaleBatchWriter.hpp`
- `timescale/schema.sql`
- `simulator/main.cpp`
- `collectors/regional/main.cpp`
- `aggregator/hierarchical/main.cpp`
- `timescale_writer/main.cpp`
- `demo.sh`

## 22. Final Recommendation

Do not treat this as a rewrite of the architecture.
Treat it as a domain conversion.

The current system is already structurally close to a fleet telemetry platform. The main tasks are:
- rename the data model
- redesign the schema
- update the simulator
- tighten validation for hardware values
- add alert and incident semantics
- keep the hierarchical transport and batching model intact

That is the cleanest, lowest-risk path.
