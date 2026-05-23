**Hardware TimescaleDB - Setup Guide

Purpose:
- Create a dedicated PostgreSQL database and role for the hardware telemetry pipeline.
- Enable the TimescaleDB extension in that database.
- Apply the hardware schema located at `timescale/hardware_schema.sql`.

Prerequisites:
- PostgreSQL server installed and running.
- TimescaleDB package available for your PostgreSQL version (installation step included below if needed).
- You have sudo access to the machine or a postgres superuser account.
- The repository is checked out locally and you are at the repo root.

Files used:
- `timescale/hardware_schema.sql` (contains the schema to apply)

Quick plan (what you will run):
1. Create a dedicated DB role (user) for writes.
2. Create the `hardware_telemetry` database owned by that role.
3. Enable `timescaledb` extension in that database.
4. Apply `timescale/hardware_schema.sql` to create tables, hypertables, indexes.

Commands
--------
(1) Optional: install TimescaleDB package (Debian/Ubuntu example)

The package name is versioned and comes from the Timescale repository, so `timescaledb-postgresql-15`
will fail on systems that do not have that repo configured.

```bash
# Install prerequisites for adding the PostgreSQL + Timescale package repos
sudo apt update
sudo apt install -y gnupg postgresql-common apt-transport-https lsb-release wget

# Add the PostgreSQL apt repository helper (needed on Debian/Ubuntu)
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh

# Add the TimescaleDB package repository
echo "deb https://packagecloud.io/timescale/timescaledb/debian/ $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/timescaledb.list
wget --quiet -O - https://packagecloud.io/timescale/timescaledb/gpgkey | sudo gpg --dearmor -o /etc/apt/trusted.gpg.d/timescaledb.gpg

sudo apt update

# Install the package that matches your PostgreSQL major version
# Example: PostgreSQL 15
sudo apt install -y timescaledb-2-postgresql-15 postgresql-client-15 timescaledb-tools

# Tune PostgreSQL for TimescaleDB (interactive; safe to skip during first setup)
sudo timescaledb-tune

sudo systemctl restart postgresql
```

If you are using a different PostgreSQL major version, replace `15` in `timescaledb-2-postgresql-15`
and `postgresql-client-15` with your installed major version.

(2) Create role + database as the postgres superuser

```bash
# Replace 'changeme' with a strong password
sudo -u postgres psql -c "CREATE ROLE hw_writer WITH LOGIN PASSWORD 'changeme';"
sudo -u postgres psql -c "CREATE DATABASE hardware_telemetry OWNER hw_writer;"
```

(3) Enable TimescaleDB extension in the new database

```bash
# Run as a superuser (postgres)
sudo -u postgres psql -d hardware_telemetry -c "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"
```

(4) Apply the hardware schema SQL from this repo

```bash
# From the repository root
# Option A: run as postgres superuser
sudo -u postgres psql -d hardware_telemetry -f timescale/hardware_schema.sql

# Option B: run as the created role using a connection string
export TIMESCALEDB_DSN="postgresql://hw_writer:changeme@127.0.0.1:5432/hardware_telemetry"
psql "$TIMESCALEDB_DSN" -f timescale/hardware_schema.sql
```

Verification
------------
- Check that extension exists and hypertables were created:

```bash
# List installed extensions in the DB
sudo -u postgres psql -d hardware_telemetry -c "\dx"

# Check Timescale hypertables
sudo -u postgres psql -d hardware_telemetry -c "SELECT * FROM timescaledb_information.hypertables;"
```

- Confirm tables exist:

```bash
sudo -u postgres psql -d hardware_telemetry -c "\dt+"
```

Security & operational notes
----------------------------
- Use a strong password for `hw_writer` and prefer to create a role with limited privileges.
- For production, configure `pg_hba.conf` to restrict access and consider using TLS connections.
- You may want to create a separate superuser-managed schema for maintenance and a least-privilege role for the writer.
- If you run the schema file multiple times during development, drop objects carefully (or drop the DB and re-create) rather than editing the SQL file to avoid partial state.

PSQL tips
---------
- Use `sudo -u postgres psql -d hardware_telemetry` to get an interactive prompt as the superuser.
- Use `\?` and `\h` inside psql for help with meta-commands and SQL help.

Undo (development only)
-----------------------
- To remove and recreate the DB during development:

```bash
sudo -u postgres psql -c "DROP DATABASE IF EXISTS hardware_telemetry;"
sudo -u postgres psql -c "DROP ROLE IF EXISTS hw_writer;"
```

Troubleshooting
--------------
- If `apt` says it cannot locate `timescaledb-2-postgresql-15`, the Timescale repository is not configured or your PostgreSQL major version is different.
- Check your PostgreSQL major version with `psql -V` or `postgres -V` and install the matching `timescaledb-2-postgresql-<major>` package.
- If `CREATE EXTENSION timescaledb` still returns zero rows in `pg_extension`, verify that you installed the package on the same server instance that runs PostgreSQL and that you restarted the service after installation.

If you'd like, I can also produce a small bash script (`scripts/create_hardware_db.sh`) to run these steps idempotently — want that created?