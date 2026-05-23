#pragma once

#include <fstream>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <iostream>
#include <cstdlib>
#include <map>
#include <mutex>
#include <string>

#include "../protocol/MessageEnvelope.hpp"

class ValidationAggregationConsumerPipeline {
public:
    explicit ValidationAggregationConsumerPipeline(std::string consumerName)
        : consumerName_(std::move(consumerName)) {}

    ValidationAggregationConsumerPipeline(std::string consumerName,
                                          std::function<void(const MessageEnvelope&)> onSuccess)
        : consumerName_(std::move(consumerName)), onSuccess_(std::move(onSuccess)) {}

    bool consume(const MessageEnvelope& envelope) {
        auto packet = telemetry_packet_from_envelope(envelope);
        if (!packet.has_value()) {
            std::cerr << "[" << consumerName_ << "] Invalid telemetry envelope " << envelope.message_id << std::endl;
            return false;
        }

        if (!validatePacket(*packet)) {
            std::string id = !packet->node_id.empty() ? packet->node_id : packet->hostname;
            std::cerr << "[" << consumerName_ << "] Validation failed for " << id << std::endl;
            return false;
        }

        recordPacket(*packet);
        logPacket(envelope);
        printSummary(*packet);

        if (onSuccess_) {
            onSuccess_(envelope);
        }

        return true;
    }

private:
    bool validatePacket(const TelemetryPacket& packet) const {
        if (packet.node_id.empty() && packet.hostname.empty()) return false;
        if (packet.cpu_temp_c < -40 || packet.cpu_temp_c > 200) return false;
        if (packet.cpu_util_pct < 0 || packet.cpu_util_pct > 100) return false;
        if (packet.gpu_util_pct < 0 || packet.gpu_util_pct > 100) return false;
        if (packet.mem_pressure_pct < 0 || packet.mem_pressure_pct > 100) return false;
        if (packet.fan_rpm < 0) return false;
        if (packet.health_score < 0 || packet.health_score > 100) return false;
        return true;
    }

    void recordPacket(const TelemetryPacket& packet) {
        std::lock_guard<std::mutex> lock(mutex_);
        ++totalPackets_;
        std::string key = !packet.node_id.empty() ? packet.node_id : packet.hostname;
        ++packetsBySource_[key];
        cpuTempSum_ += packet.cpu_temp_c;
    }

    void logPacket(const MessageEnvelope& envelope) {
        namespace fs = std::filesystem;
        fs::path logDir = "logs";
        if (const char* logDirEnv = std::getenv("LOGDIR"); logDirEnv != nullptr && *logDirEnv != '\0') {
            logDir = logDirEnv;
        }
        std::error_code ec;
        fs::create_directories(logDir, ec);

        fs::path logPath = logDir / (consumerName_ + ".log");
        std::ofstream logFile(logPath, std::ios::app);
        if (!logFile.is_open()) {
            return;
        }

        logFile << envelope_to_json(envelope).dump() << std::endl;
    }

    void printSummary(const TelemetryPacket& packet) {
        std::lock_guard<std::mutex> lock(mutex_);
        const double averageTemperature = totalPackets_ > 0 ? cpuTempSum_ / static_cast<double>(totalPackets_) : 0.0;
        std::string id = !packet.node_id.empty() ? packet.node_id : packet.hostname;
        std::cout << "[" << consumerName_ << "] Consumed " << id
                  << " | total=" << totalPackets_
                  << " | source_count=" << packetsBySource_[id]
                  << " | avg_temp=" << std::fixed << std::setprecision(2) << averageTemperature
                  << std::endl;
    }

    std::string consumerName_;
    std::mutex mutex_;
    std::size_t totalPackets_{0};
    std::map<std::string, std::size_t> packetsBySource_;
    double cpuTempSum_{0.0};
    std::function<void(const MessageEnvelope&)> onSuccess_;
};