#pragma once

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#if __has_include(<libpq-fe.h>)
#include <libpq-fe.h>
#define WEATHER_RTOS_HAS_LIBPQ 1
#elif __has_include(<postgresql/libpq-fe.h>)
#include <postgresql/libpq-fe.h>
#define WEATHER_RTOS_HAS_LIBPQ 1
#else
#define WEATHER_RTOS_HAS_LIBPQ 0
#endif

class TimescaleDbClient {
public:
    TimescaleDbClient() {
        // First, check for explicit environment configuration
        const char* dsn = std::getenv("TIMESCALEDB_DSN");
        const char* connectionString = std::getenv("TIMESCALEDB_CONNECTION_STRING");

        if (dsn != nullptr && *dsn != '\0') {
            connect(dsn);
            return;
        }

        if (connectionString != nullptr && *connectionString != '\0') {
            connect(connectionString);
            return;
        }

        // Auto-detect: try the repo's local TimescaleDB setup first, then common fallbacks.
        const std::string localConnStrings[] = {
            "host=127.0.0.1 port=5432 dbname=weather_rtos user=weather_rtos password=weather_secret",
            "postgresql://weather_rtos:weather_secret@127.0.0.1:5432/weather_rtos",
            "postgresql://weather_rtos:weather_secret@localhost:5432/weather_rtos",
            "postgresql://postgres:postgres@localhost:5432/weather_db",
            "postgresql://postgres@localhost:5432/weather_db",
            "postgresql://localhost:5432/weather_db",
            "postgresql://127.0.0.1:5432/weather_db",
            "host=localhost port=5432 user=postgres password=postgres dbname=weather_db",
            "host=localhost port=5432 user=postgres dbname=weather_db",
        };

        std::cout << "[TimescaleDbClient] Attempting auto-detection of local TimescaleDB..." << std::endl;
        for (const auto& connStr : localConnStrings) {
            if (tryConnect(connStr)) {
                return;  // Successfully connected
            }
        }

        std::cout << "[TimescaleDbClient] Could not auto-detect TimescaleDB, will use file outbox fallback" << std::endl;
    }

    ~TimescaleDbClient() {
#if WEATHER_RTOS_HAS_LIBPQ
        if (connection_ != nullptr) {
            PQfinish(connection_);
            connection_ = nullptr;
        }
#endif
    }

    bool isEnabled() const {
        return enabled_;
    }

    bool isConnected() const {
#if WEATHER_RTOS_HAS_LIBPQ
        return connection_ != nullptr && PQstatus(connection_) == CONNECTION_OK;
#else
        return enabled_ && !connectionString_.empty();
#endif
    }

    std::string backendName() const {
#if WEATHER_RTOS_HAS_LIBPQ
        return isConnected() ? "TimescaleDB" : "file outbox";
#else
        return connectionString_.empty() ? "file outbox" : "TimescaleDB";
#endif
    }

    bool executeBatch(const std::string& sql) {
#if WEATHER_RTOS_HAS_LIBPQ
        if (!isConnected()) {
            return false;
        }

        PGresult* result = PQexec(connection_, sql.c_str());
        if (result == nullptr) {
            std::cerr << "[TimescaleDB] PQexec returned null: " << PQerrorMessage(connection_) << std::endl;
            rollback();
            return false;
        }

        const ExecStatusType status = PQresultStatus(result);
        const bool ok = (status == PGRES_COMMAND_OK) || (status == PGRES_TUPLES_OK);
        if (!ok) {
            std::cerr << "[TimescaleDB] Batch failed: " << PQresultErrorMessage(result) << std::endl;
            PQclear(result);
            rollback();
            return false;
        }

        PQclear(result);
        return true;
#else
    if (connectionString_.empty()) {
            return false;
        }

        return runPsqlBatch(connectionString_, sql);
#endif
    }

private:
    bool tryConnect(const std::string& conninfo) {
#if WEATHER_RTOS_HAS_LIBPQ
        PGconn* testConn = PQconnectdb(conninfo.c_str());
        if (testConn == nullptr) {
            return false;
        }

        const ConnStatusType status = PQstatus(testConn);
        if (status == CONNECTION_OK) {
            // Success! Store the connection
            connection_ = testConn;
            enabled_ = true;
            std::cout << "[TimescaleDB] ✓ Connected successfully to: " << conninfo << std::endl;
            return true;
        }

        // Failed
        PQfinish(testConn);
        return false;
#else
        if (!probePsql(conninfo)) {
            return false;
        }

        connectionString_ = conninfo;
        enabled_ = true;
        std::cout << "[TimescaleDB] ✓ Connected successfully to: " << conninfo << std::endl;
        return true;
#endif
    }

    void connect(const std::string& conninfo) {
        enabled_ = true;
#if WEATHER_RTOS_HAS_LIBPQ
        connection_ = PQconnectdb(conninfo.c_str());
        if (connection_ == nullptr || PQstatus(connection_) != CONNECTION_OK) {
            std::cerr << "[TimescaleDB] Connection failed: "
                      << (connection_ != nullptr ? PQerrorMessage(connection_) : "unknown error")
                      << std::endl;
            if (connection_ != nullptr) {
                PQfinish(connection_);
                connection_ = nullptr;
            }
        }
#else
        connectionString_ = conninfo;
        if (!probePsql(conninfo)) {
            std::cerr << "[TimescaleDB] Connection failed via psql fallback: " << conninfo << std::endl;
            enabled_ = false;
            connectionString_.clear();
        }
#endif
    }

#if !WEATHER_RTOS_HAS_LIBPQ
    static bool probePsql(const std::string& conninfo) {
        const std::string command = "psql -X -q -v ON_ERROR_STOP=1 -d \"" + conninfo + "\" -Atqc 'select 1;' >/dev/null 2>&1";
        const int status = std::system(command.c_str());
        return WIFEXITED(status) && WEXITSTATUS(status) == 0;
    }

    static bool runPsqlBatch(const std::string& conninfo, const std::string& sql) {
        int pipeFd[2];
        if (pipe(pipeFd) != 0) {
            std::cerr << "[TimescaleDB] pipe() failed: " << std::strerror(errno) << std::endl;
            return false;
        }

        const pid_t pid = fork();
        if (pid < 0) {
            std::cerr << "[TimescaleDB] fork() failed: " << std::strerror(errno) << std::endl;
            close(pipeFd[0]);
            close(pipeFd[1]);
            return false;
        }

        if (pid == 0) {
            dup2(pipeFd[0], STDIN_FILENO);
            close(pipeFd[0]);
            close(pipeFd[1]);
            execlp("psql",
                   "psql",
                   "-X",
                   "-q",
                   "-v",
                   "ON_ERROR_STOP=1",
                   "-d",
                   conninfo.c_str(),
                   static_cast<char*>(nullptr));
            _exit(127);
        }

        close(pipeFd[0]);
        const ssize_t written = write(pipeFd[1], sql.data(), static_cast<size_t>(sql.size()));
        close(pipeFd[1]);

        int waitStatus = 0;
        if (waitpid(pid, &waitStatus, 0) < 0) {
            std::cerr << "[TimescaleDB] waitpid() failed: " << std::strerror(errno) << std::endl;
            return false;
        }

        if (written < 0) {
            std::cerr << "[TimescaleDB] write() failed: " << std::strerror(errno) << std::endl;
            return false;
        }

        return WIFEXITED(waitStatus) && WEXITSTATUS(waitStatus) == 0;
    }
#endif

#if WEATHER_RTOS_HAS_LIBPQ
    void rollback() {
        if (isConnected()) {
            PGresult* result = PQexec(connection_, "ROLLBACK;");
            if (result != nullptr) {
                PQclear(result);
            }
        }
    }

    PGconn* connection_{nullptr};
#endif

    std::string connectionString_;
    bool enabled_{false};
};