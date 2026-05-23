#pragma once

#include <chrono>
#include <condition_variable>
#include <atomic>
#include <algorithm>
#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <nlohmann/json.hpp>

#include "../models/WeatherPacket.hpp"
#include "../protocol/MessageEnvelope.hpp"
#include "TimescaleDbClient.hpp"

using json = nlohmann::json;

struct TimescaleRawRow {
    std::string message_id;
    std::string source;
    std::string continent;
    std::string country;
    std::string region;
    std::string city;
    long event_time;
    long created_at;
    double temperature;
    double humidity;
    double wind_speed;
    std::string payload_json;
};

struct TimescaleAggregateRow {
    long bucket_start;
    std::string continent;
    std::string country;
    std::string region;
    std::string city;
    std::size_t observation_count;
    double min_temperature;
    double max_temperature;
    double avg_temperature;
    double avg_humidity;
    double avg_wind_speed;
};

class TimescaleBatchWriter {
public:
    TimescaleBatchWriter(std::string outboxPath,
                         std::size_t maxQueueSize = 1024,
                         std::size_t batchSize = 100,
                         std::chrono::milliseconds flushInterval = std::chrono::milliseconds(1000))
        : outboxPath_(std::move(outboxPath)),
          maxQueueSize_(maxQueueSize),
          batchSize_(batchSize),
                    flushInterval_(flushInterval),
                    timescaleDb_() {}

    bool submit(const MessageEnvelope& envelope) {
        auto packet = weather_packet_from_envelope(envelope);
        if (!packet.has_value()) {
            std::cerr << "[TimescaleWriter] Rejected invalid envelope " << envelope.message_id << std::endl;
            return false;
        }

        std::unique_lock<std::mutex> lock(mutex_);
        if (pending_.size() >= maxQueueSize_) {
            std::cerr << "[TimescaleWriter] Backpressure: queue full, rejecting " << envelope.message_id << std::endl;
            return false;
        }

        if (!seenIds_.insert(envelope.message_id).second) {
            std::cout << "[TimescaleWriter] Duplicate envelope ignored: " << envelope.message_id << std::endl;
            return true;
        }

        pending_.push_back(std::move(envelope));
        cv_.notify_one();
        return true;
    }

    void run(std::atomic<bool>& running) {
        std::cout << "[TimescaleWriter] Batch writer started, backend: " << timescaleDb_.backendName() << std::endl;
        if (timescaleDb_.backendName() == "file outbox") {
            std::cout << "[TimescaleWriter] Outbox fallback: " << outboxPath_ << std::endl;
        }

        while (running.load()) {
            std::vector<MessageEnvelope> batch;

            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait_for(lock, flushInterval_, [&]() {
                    return !pending_.empty() || !running.load();
                });

                while (!pending_.empty() && batch.size() < batchSize_) {
                    batch.push_back(std::move(pending_.front()));
                    pending_.pop_front();
                }
            }

            if (!batch.empty()) {
                flushBatch(batch);
            }
        }

        // Final drain
        std::vector<MessageEnvelope> finalBatch;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            while (!pending_.empty()) {
                finalBatch.push_back(std::move(pending_.front()));
                pending_.pop_front();
            }
        }

        if (!finalBatch.empty()) {
            flushBatch(finalBatch);
        }

        std::cout << "[TimescaleWriter] Batch writer stopped" << std::endl;
    }

private:
    static std::string escapeSqlLiteral(const std::string& value) {
        std::string escaped;
        escaped.reserve(value.size() + 8);
        for (char ch : value) {
            if (ch == '\'') {
                escaped += "''";
            } else {
                escaped.push_back(ch);
            }
        }
        return escaped;
    }

    static long minuteBucket(long timestampMs) {
        return (timestampMs / 60000L) * 60000L;
    }

    static long toEpochMs(long rawTimestamp) {
        // WeatherPacket timestamps are already epoch-millisecond values in this pipeline.
        return rawTimestamp;
    }

    void flushBatch(const std::vector<MessageEnvelope>& batch) {
        std::vector<TimescaleRawRow> rawRows;
        rawRows.reserve(batch.size());

        std::map<std::tuple<long, std::string, std::string, std::string, std::string>, std::vector<WeatherPacket>> aggregateGroups;

        for (const auto& envelope : batch) {
            auto packetOpt = weather_packet_from_envelope(envelope);
            if (!packetOpt.has_value()) {
                continue;
            }

            const WeatherPacket& packet = *packetOpt;
            const long eventTime = packet.timestamp;
            rawRows.push_back({
                envelope.message_id,
                envelope.source,
                packet.continent,
                packet.country,
                packet.region,
                packet.city,
                eventTime,
                envelope.created_at,
                packet.temperature,
                packet.humidity,
                packet.wind_speed,
                envelope_to_json(envelope).dump()
            });

            aggregateGroups[{minuteBucket(eventTime), packet.continent, packet.country, packet.region, packet.city}].push_back(packet);
        }

        std::vector<TimescaleAggregateRow> aggregateRows;
        aggregateRows.reserve(aggregateGroups.size());

        for (auto& entry : aggregateGroups) {
            const auto& key = entry.first;
            const auto& packets = entry.second;
            if (packets.empty()) {
                continue;
            }

            double minTemp = packets.front().temperature;
            double maxTemp = packets.front().temperature;
            double sumTemp = 0.0;
            double sumHumidity = 0.0;
            double sumWind = 0.0;

            for (const auto& packet : packets) {
                minTemp = std::min(minTemp, packet.temperature);
                maxTemp = std::max(maxTemp, packet.temperature);
                sumTemp += packet.temperature;
                sumHumidity += packet.humidity;
                sumWind += packet.wind_speed;
            }

            aggregateRows.push_back({
                std::get<0>(key),
                std::get<1>(key),
                std::get<2>(key),
                std::get<3>(key),
                std::get<4>(key),
                packets.size(),
                minTemp,
                maxTemp,
                sumTemp / static_cast<double>(packets.size()),
                sumHumidity / static_cast<double>(packets.size()),
                sumWind / static_cast<double>(packets.size())
            });
        }

        const std::string batchSql = buildBatchSql(rawRows, aggregateRows);

        if (timescaleDb_.executeBatch(batchSql)) {
            std::cout << "[TimescaleWriter] Flushed to TimescaleDB raw=" << rawRows.size()
                      << " aggregate=" << aggregateRows.size() << std::endl;
            return;
        }

        std::ofstream outbox(outboxPath_, std::ios::app);
        if (!outbox.is_open()) {
            std::cerr << "[TimescaleWriter] Failed to open outbox file " << outboxPath_ << std::endl;
            return;
        }

        outbox << batchSql << std::endl;
        std::cout << "[TimescaleWriter] Flushed to outbox raw=" << rawRows.size()
                  << " aggregate=" << aggregateRows.size() << std::endl;
    }

    std::string buildBatchSql(const std::vector<TimescaleRawRow>& rawRows,
                             const std::vector<TimescaleAggregateRow>& aggregateRows) {
        std::ostringstream sql;
        sql << "-- batch " << std::chrono::system_clock::now().time_since_epoch().count() << std::endl;
        sql << "BEGIN;" << std::endl;

        for (const auto& row : rawRows) {
            sql << "INSERT INTO weather_observations_raw "
                << "(message_id, source, continent, country, region, city, event_time, created_at, temperature, humidity, wind_speed, payload_json) VALUES ("
                << "'" << escapeSqlLiteral(row.message_id) << "', '"
                << escapeSqlLiteral(row.source) << "', '"
                << escapeSqlLiteral(row.continent) << "', '"
                << escapeSqlLiteral(row.country) << "', '"
                << escapeSqlLiteral(row.region) << "', '"
                << escapeSqlLiteral(row.city) << "', to_timestamp(" << row.event_time << " / 1000.0), to_timestamp(" << row.created_at << " / 1000.0), "
                << row.temperature << ", " << row.humidity << ", " << row.wind_speed << ", '"
                << escapeSqlLiteral(row.payload_json) << "') ON CONFLICT (message_id) DO NOTHING;" << std::endl;
        }

        for (const auto& row : aggregateRows) {
            sql << "INSERT INTO weather_city_minute_aggregates "
                << "(bucket_start, continent, country, region, city, observation_count, min_temperature, max_temperature, avg_temperature, avg_humidity, avg_wind_speed) VALUES ("
                << "to_timestamp(" << row.bucket_start << " / 1000.0), '"
                << escapeSqlLiteral(row.continent) << "', '"
                << escapeSqlLiteral(row.country) << "', '"
                << escapeSqlLiteral(row.region) << "', '"
                << escapeSqlLiteral(row.city) << "', "
                << row.observation_count << ", "
                << row.min_temperature << ", "
                << row.max_temperature << ", "
                << row.avg_temperature << ", "
                << row.avg_humidity << ", "
                << row.avg_wind_speed << ") ON CONFLICT (bucket_start, continent, country, region, city) DO UPDATE SET "
                << "observation_count = EXCLUDED.observation_count, "
                << "min_temperature = LEAST(weather_city_minute_aggregates.min_temperature, EXCLUDED.min_temperature), "
                << "max_temperature = GREATEST(weather_city_minute_aggregates.max_temperature, EXCLUDED.max_temperature), "
                << "avg_temperature = EXCLUDED.avg_temperature, "
                << "avg_humidity = EXCLUDED.avg_humidity, "
                << "avg_wind_speed = EXCLUDED.avg_wind_speed;" << std::endl;
        }

        sql << "COMMIT;" << std::endl;
        return sql.str();
    }

    std::string outboxPath_;
    std::size_t maxQueueSize_;
    std::size_t batchSize_;
    std::chrono::milliseconds flushInterval_;
    std::deque<MessageEnvelope> pending_;
    std::unordered_set<std::string> seenIds_;
    std::mutex mutex_;
    std::condition_variable cv_;
    TimescaleDbClient timescaleDb_;
};
