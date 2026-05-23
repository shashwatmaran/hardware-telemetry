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
        auto packet = weather_packet_from_envelope(envelope);
        if (!packet.has_value()) {
            std::cerr << "[" << consumerName_ << "] Invalid weather envelope " << envelope.message_id << std::endl;
            return false;
        }

        if (!validatePacket(*packet)) {
            std::cerr << "[" << consumerName_ << "] Validation failed for " << packet->city << std::endl;
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
    bool validatePacket(const WeatherPacket& packet) const {
        if (packet.continent.empty() || packet.country.empty() || packet.region.empty() || packet.city.empty()) {
            return false;
        }
        if (packet.temperature < -100 || packet.temperature > 100) {
            return false;
        }
        if (packet.humidity < 0 || packet.humidity > 100) {
            return false;
        }
        if (packet.wind_speed < 0 || packet.wind_speed > 200) {
            return false;
        }
        return true;
    }

    void recordPacket(const WeatherPacket& packet) {
        std::lock_guard<std::mutex> lock(mutex_);
        ++totalPackets_;
        ++packetsByCity_[packet.city];
        temperatureSum_ += packet.temperature;
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

    void printSummary(const WeatherPacket& packet) {
        std::lock_guard<std::mutex> lock(mutex_);
        const double averageTemperature = totalPackets_ > 0 ? temperatureSum_ / static_cast<double>(totalPackets_) : 0.0;
        std::cout << "[" << consumerName_ << "] Consumed " << packet.city
                  << " | total=" << totalPackets_
                  << " | city_count=" << packetsByCity_[packet.city]
                  << " | avg_temp=" << std::fixed << std::setprecision(2) << averageTemperature
                  << std::endl;
    }

    std::string consumerName_;
    std::mutex mutex_;
    std::size_t totalPackets_{0};
    std::map<std::string, std::size_t> packetsByCity_;
    double temperatureSum_{0.0};
    std::function<void(const MessageEnvelope&)> onSuccess_;
};