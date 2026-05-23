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

#include "../models/TelemetryPacket.hpp"
#include "../protocol/MessageEnvelope.hpp"
#include "TimescaleDbClient.hpp"

using json = nlohmann::json;

struct TimescaleRawRow {
    std::string message_id;
    std::string source;
    std::string node_id;
    std::string asset_tag;
    std::string hostname;
    std::string site_id;
    std::string rack_id;
    std::string region;
    std::string geo_region;
    long event_time;
    long created_at;

    double cpu_temp_c;
    double cpu_util_pct;
    double gpu_temp_c;
    double gpu_util_pct;
    double vram_used_mb;
    double vram_total_mb;
    double mem_used_mb;
    double mem_total_mb;
    double mem_pressure_pct;
    double power_draw_w;
    double fan_rpm;
    bool thermal_throttle_active;
    std::string thermal_throttle_reason;
    double disk_util_pct;
    double nvme_temp_c;
    double net_tx_mbps;
    double net_rx_mbps;
    double net_latency_ms;
    double packet_loss_pct;
    long ecc_error_count;
    long pcie_error_count;
    double health_score;
    std::string status_flags;
    std::string source_vendor;
    std::string source_model;
    std::string payload_json;
};

struct TimescaleAggregateRow {
    long bucket_start;
    std::string node_id;
    std::string site_id;
    std::string region;
    std::string geo_region;
    std::size_t sample_count;
    double min_cpu_temp_c;
    double max_cpu_temp_c;
    double avg_cpu_temp_c;
    double avg_cpu_util_pct;
    double avg_gpu_util_pct;
    double avg_vram_used_pct;
    double avg_mem_pressure_pct;
    double avg_power_draw_w;
    double avg_fan_rpm;
    std::size_t throttle_count;
    std::size_t alert_count;
    double health_score_avg;
    double health_score_min;
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
        auto packet = telemetry_packet_from_envelope(envelope);
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
        // TelemetryPacket timestamps are epoch-millisecond values in this pipeline.
        return rawTimestamp;
    }

    void flushBatch(const std::vector<MessageEnvelope>& batch) {
        std::vector<TimescaleRawRow> rawRows;
        rawRows.reserve(batch.size());

        std::map<std::tuple<long, std::string, std::string, std::string, std::string>, std::vector<TelemetryPacket>> aggregateGroups;

        for (const auto& envelope : batch) {
            auto packetOpt = telemetry_packet_from_envelope(envelope);
            if (!packetOpt.has_value()) {
                continue;
            }

            const TelemetryPacket& packet = *packetOpt;
            const long eventTime = packet.timestamp;
            rawRows.push_back({
                envelope.message_id,
                envelope.source,
                packet.node_id,
                packet.asset_tag,
                packet.hostname,
                packet.site_id,
                packet.rack_id,
                packet.region,
                packet.geo_region,
                eventTime,
                envelope.created_at,

                packet.cpu_temp_c,
                packet.cpu_util_pct,
                packet.gpu_temp_c,
                packet.gpu_util_pct,
                packet.vram_used_mb,
                packet.vram_total_mb,
                packet.mem_used_mb,
                packet.mem_total_mb,
                packet.mem_pressure_pct,
                packet.power_draw_w,
                packet.fan_rpm,
                packet.thermal_throttle_active,
                packet.thermal_throttle_reason,
                packet.disk_util_pct,
                packet.nvme_temp_c,
                packet.net_tx_mbps,
                packet.net_rx_mbps,
                packet.net_latency_ms,
                packet.packet_loss_pct,
                packet.ecc_error_count,
                packet.pcie_error_count,
                packet.health_score,
                packet.status_flags,
                packet.source_vendor,
                packet.source_model,
                envelope_to_json(envelope).dump()
            });

            aggregateGroups[{minuteBucket(eventTime), packet.node_id, packet.site_id, packet.region, packet.geo_region}].push_back(packet);
        }

        std::vector<TimescaleAggregateRow> aggregateRows;
        aggregateRows.reserve(aggregateGroups.size());

        for (auto& entry : aggregateGroups) {
            const auto& key = entry.first;
            const auto& packets = entry.second;
            if (packets.empty()) {
                continue;
            }
            double minCpu = packets.front().cpu_temp_c;
            double maxCpu = packets.front().cpu_temp_c;
            double sumCpu = 0.0;
            double sumCpuUtil = 0.0;
            double sumGpuUtil = 0.0;
            double sumVramUsedPct = 0.0;
            double sumMemPressure = 0.0;
            double sumPower = 0.0;
            double sumFan = 0.0;
            double sumHealth = 0.0;

            for (const auto& packet : packets) {
                minCpu = std::min(minCpu, packet.cpu_temp_c);
                maxCpu = std::max(maxCpu, packet.cpu_temp_c);
                sumCpu += packet.cpu_temp_c;
                sumCpuUtil += packet.cpu_util_pct;
                sumGpuUtil += packet.gpu_util_pct;
                sumVramUsedPct += (packet.vram_total_mb > 0.0) ? (packet.vram_used_mb / packet.vram_total_mb * 100.0) : 0.0;
                sumMemPressure += packet.mem_pressure_pct;
                sumPower += packet.power_draw_w;
                sumFan += packet.fan_rpm;
                sumHealth += packet.health_score;
            }

            aggregateRows.push_back({
                std::get<0>(key),
                std::get<1>(key),
                std::get<2>(key),
                std::get<3>(key),
                std::get<4>(key),
                packets.size(),
                minCpu,
                maxCpu,
                sumCpu / static_cast<double>(packets.size()),
                sumCpuUtil / static_cast<double>(packets.size()),
                sumGpuUtil / static_cast<double>(packets.size()),
                sumVramUsedPct / static_cast<double>(packets.size()),
                sumMemPressure / static_cast<double>(packets.size()),
                sumPower / static_cast<double>(packets.size()),
                sumFan / static_cast<double>(packets.size()),
                0,
                0,
                sumHealth / static_cast<double>(packets.size()),
                0.0
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
            sql << "INSERT INTO hardware_telemetry_raw "
                << "(message_id, source, node_id, asset_tag, hostname, site_id, rack_id, region, geo_region, event_time, created_at, cpu_temp_c, cpu_util_pct, gpu_temp_c, gpu_util_pct, vram_used_mb, vram_total_mb, mem_used_mb, mem_total_mb, mem_pressure_pct, power_draw_w, fan_rpm, thermal_throttle_active, thermal_throttle_reason, disk_util_pct, nvme_temp_c, net_tx_mbps, net_rx_mbps, net_latency_ms, packet_loss_pct, ecc_error_count, pcie_error_count, health_score, status_flags, source_vendor, source_model, payload_json) VALUES ("
                << "'" << escapeSqlLiteral(row.message_id) << "', '"
                << escapeSqlLiteral(row.source) << "', '"
                << escapeSqlLiteral(row.node_id) << "', '"
                << escapeSqlLiteral(row.asset_tag) << "', '"
                << escapeSqlLiteral(row.hostname) << "', '"
                << escapeSqlLiteral(row.site_id) << "', '"
                << escapeSqlLiteral(row.rack_id) << "', '"
                << escapeSqlLiteral(row.region) << "', '"
                << escapeSqlLiteral(row.geo_region) << "', to_timestamp(" << row.event_time << " / 1000.0), to_timestamp(" << row.created_at << " / 1000.0), "
                << row.cpu_temp_c << ", " << row.cpu_util_pct << ", " << row.gpu_temp_c << ", " << row.gpu_util_pct << ", "
                << row.vram_used_mb << ", " << row.vram_total_mb << ", " << row.mem_used_mb << ", " << row.mem_total_mb << ", "
                << row.mem_pressure_pct << ", " << row.power_draw_w << ", " << row.fan_rpm << ", " << (row.thermal_throttle_active ? "TRUE" : "FALSE") << ", '"
                << escapeSqlLiteral(row.thermal_throttle_reason) << "', " << row.disk_util_pct << ", " << row.nvme_temp_c << ", "
                << row.net_tx_mbps << ", " << row.net_rx_mbps << ", " << row.net_latency_ms << ", " << row.packet_loss_pct << ", "
                << row.ecc_error_count << ", " << row.pcie_error_count << ", " << row.health_score << ", '"
                << escapeSqlLiteral(row.status_flags) << "', '" << escapeSqlLiteral(row.source_vendor) << "', '" << escapeSqlLiteral(row.source_model) << "', '"
                << escapeSqlLiteral(row.payload_json) << "') ON CONFLICT (message_id) DO NOTHING;" << std::endl;
        }

        for (const auto& row : aggregateRows) {
            sql << "INSERT INTO node_minute_aggregates "
                << "(bucket_start, node_id, site_id, region, geo_region, sample_count, min_cpu_temp_c, max_cpu_temp_c, avg_cpu_temp_c, avg_cpu_util_pct, avg_gpu_util_pct, avg_vram_used_pct, avg_mem_pressure_pct, avg_power_draw_w, avg_fan_rpm, throttle_count, alert_count, health_score_avg, health_score_min) VALUES ("
                << "to_timestamp(" << row.bucket_start << " / 1000.0), '"
                << escapeSqlLiteral(row.node_id) << "', '"
                << escapeSqlLiteral(row.site_id) << "', '"
                << escapeSqlLiteral(row.region) << "', '"
                << escapeSqlLiteral(row.geo_region) << "', "
                << row.sample_count << ", "
                << row.min_cpu_temp_c << ", "
                << row.max_cpu_temp_c << ", "
                << row.avg_cpu_temp_c << ", "
                << row.avg_cpu_util_pct << ", "
                << row.avg_gpu_util_pct << ", "
                << row.avg_vram_used_pct << ", "
                << row.avg_mem_pressure_pct << ", "
                << row.avg_power_draw_w << ", "
                << row.avg_fan_rpm << ", "
                << row.throttle_count << ", "
                << row.alert_count << ", "
                << row.health_score_avg << ", "
                << row.health_score_min << ") ON CONFLICT (bucket_start, node_id) DO UPDATE SET "
                << "sample_count = EXCLUDED.sample_count, "
                << "min_cpu_temp_c = LEAST(node_minute_aggregates.min_cpu_temp_c, EXCLUDED.min_cpu_temp_c), "
                << "max_cpu_temp_c = GREATEST(node_minute_aggregates.max_cpu_temp_c, EXCLUDED.max_cpu_temp_c), "
                << "avg_cpu_temp_c = EXCLUDED.avg_cpu_temp_c, "
                << "avg_cpu_util_pct = EXCLUDED.avg_cpu_util_pct, "
                << "avg_gpu_util_pct = EXCLUDED.avg_gpu_util_pct, "
                << "avg_vram_used_pct = EXCLUDED.avg_vram_used_pct, "
                << "avg_mem_pressure_pct = EXCLUDED.avg_mem_pressure_pct, "
                << "avg_power_draw_w = EXCLUDED.avg_power_draw_w, "
                << "avg_fan_rpm = EXCLUDED.avg_fan_rpm, "
                << "health_score_avg = EXCLUDED.health_score_avg, "
                << "health_score_min = EXCLUDED.health_score_min;" << std::endl;
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
