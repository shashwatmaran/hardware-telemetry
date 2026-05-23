# Hardware Telemetry Quickstart

This quickstart shows the minimal steps to build and run the demo locally.

## Prerequisites
- CMake (>=3.10)
- A C++17-capable compiler such as `g++` or `clang++`
- `make` or `ninja`
- Optional: TimescaleDB if you want persistence for the writer

## Build
```bash
git clone <repo> hardware-telemetry
cd hardware-telemetry
mkdir -p build && cd build
cmake ..
cmake --build . -j 4
```

## Run Demo
```bash
cd /path/to/hardware-telemetry
./demo.sh
```

## Environment Variables
- `TIMESCALEDB_DSN` controls the TimescaleDB connection string.
- `HARDWARE_TELEMETRY_HOST` controls the TCP host used by components.
- `LOGDIR` overrides the log directory.

## Logs
- Component stdout/stderr and consumer logs are written to `logs/` by default.
- Rotate logs manually with `logs/rotate.sh`.

## Run A Single Component
```bash
./build/hierarchical_aggregator asia_continent
```

The full demo uses the hierarchical chain already wired in [demo.sh](demo.sh), so you do not need to assemble separate region-by-region trees to confirm the architecture.

## Notes
- If you do not have TimescaleDB, the writer will fall back to the outbox file.
- For development, prefer running one component and exercising inputs through the simulator or TCP socket helpers.
