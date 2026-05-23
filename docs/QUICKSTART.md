# Quickstart

This quickstart shows the minimal steps to build and run the demo locally.

Prerequisites
- CMake (>=3.10), a C++17-capable compiler (g++/clang++) and `make` or `ninja`.
- Optional: TimescaleDB (Postgres) if you want persistence for the `timescale_writer`.

Build
```bash
git clone <repo> weather-rtos
cd weather-rtos
mkdir -p build && cd build
cmake ..
cmake --build . -j 4
```

Run demo (defaults)
```bash
cd /path/to/weather-rtos
./demo.sh
```

Environment variables
- `TIMESCALEDB_DSN` — database DSN for TimescaleDB writer. Default is set in `demo.sh`.
- `WEATHER_RTOS_HOST` — network host used by components.
- `LOGDIR` — override default log location; default is `logs/` in repository root.

Inspect logs
- By default component stdout/stderr and consumer logs are written to `logs/`.
- Rotate logs manually:
```bash
logs/rotate.sh
```

Run a component individually
- You can run a single component from `build/`, e.g.:
```bash
./build/hierarchical_aggregator asia_continent
```
- The full demo uses the hierarchical chain already wired in [demo.sh](demo.sh), so you do not need to assemble separate region-by-region trees to confirm the architecture.

Notes
- If you don't have TimescaleDB, the `timescale_writer` will run but writes will fail; set `TIMESCALEDB_DSN` to a reachable DB to enable persistence.
- For development prefer running a single component and exercising its inputs via the simulator or curl/sockets.
