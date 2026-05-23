# Hardware Telemetry

This repository implements a hierarchical hardware telemetry pipeline. Regional collectors synthesize node metrics, aggregators validate and forward envelopes, and the Timescale writer stores raw samples plus minute rollups.

The main flow is collector -> regional aggregator -> continent aggregator -> global sink -> Timescale writer. Shared message, routing, publishing, and Timescale helpers live under `common/`.

Key pieces:
- `CMakeLists.txt`: top-level build configuration.
- `demo.sh`: local end-to-end demo runner.
- `timescale/hardware_schema.sql`: database schema for raw telemetry and rollups.
- `timescale_outbox.sql`: fallback batch output when the database is unavailable.
- `collectors/`: collector entrypoints.
- `aggregator/`: aggregator entrypoints.
- `simulator/main.cpp`: synthetic telemetry generator.
- `timescale_writer/main.cpp`: batch writer and outbox consumer.
- `common/models/TelemetryPacket.hpp`: telemetry payload model.
- `common/protocol/MessageEnvelope.hpp`: envelope format.
- `common/pipeline/ValidationAggregationConsumerPipeline.hpp`: validation and consumer pipeline.
- `common/timescale/`: Timescale client and batch writer helpers.

The codebase supports both in-process and TCP messaging. The telemetry payload includes node identity, site and region metadata, CPU/GPU/memory/power/network metrics, and health scoring for downstream aggregation.
