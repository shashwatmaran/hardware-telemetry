#include <iostream>
#include <fstream>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <atomic>
#include <csignal>
#include <cmath>
#include <cctype>
#include <vector>
#include <nlohmann/json.hpp>
#include "../../common/models/TelemetryPacket.hpp"
#include "../../common/utils/RuntimeConfig.hpp"
#include "../../common/protocol/MessageEnvelope.hpp"
#include "../../common/publishing/BrokerPublisher.hpp"

using json = nlohmann::json;

constexpr std::size_t MAX_QUEUE_SIZE = 100;

std::queue<TelemetryPacket> telemetryQueue;
std::mutex queueMutex;
std::condition_variable queueCv;
std::atomic<bool> running{true};

void handleShutdownSignal(int) {
    running.store(false);
}

namespace {

std::string slugify(const std::string& value) {
    std::string slug;
    slug.reserve(value.size());
    for (char ch : value) {
        unsigned char uch = static_cast<unsigned char>(ch);
        if (std::isalnum(uch)) {
            slug.push_back(static_cast<char>(std::tolower(uch)));
        } else if (!slug.empty() && slug.back() != '_') {
            slug.push_back('_');
        }
    }
    while (!slug.empty() && slug.back() == '_') {
        slug.pop_back();
    }
    return slug.empty() ? "node" : slug;
}

double wave(double phase, double scale, double offset) {
    return offset + scale * (std::sin(phase) * 0.5 + 0.5);
}

TelemetryPacket buildTelemetryPacket(const std::string& continent,
                                     const std::string& country,
                                     const std::string& region,
                                     const std::string& city,
                                     double lat,
                                     double lon,
                                     long sampleSeq) {
    TelemetryPacket packet;
    const std::string citySlug = slugify(city);
    const double phase = static_cast<double>(sampleSeq) * 0.29 + lat * 0.05 + lon * 0.02;
    const double secondaryPhase = phase * 1.23 + static_cast<double>(citySlug.size()) * 0.17;

    packet.node_id = region + ":" + citySlug;
    packet.asset_tag = region + "-" + citySlug;
    packet.hostname = citySlug + "." + slugify(region) + ".local";
    packet.site_id = slugify(country);
    packet.rack_id = "rack-" + std::to_string((sampleSeq % 8) + 1);
    packet.region = region;
    packet.geo_region = continent;
    packet.sample_seq = sampleSeq;
    packet.timestamp = epoch_millis_now();

    packet.cpu_temp_c = wave(phase, 24.0, 39.0);
    packet.cpu_util_pct = wave(secondaryPhase, 70.0, 20.0);
    packet.gpu_temp_c = wave(phase + 1.0, 22.0, 35.0);
    packet.gpu_util_pct = wave(secondaryPhase + 0.7, 65.0, 10.0);
    packet.vram_total_mb = 16384.0;
    packet.vram_used_mb = wave(phase + 0.4, 11000.0, 2500.0);
    packet.mem_total_mb = 65536.0;
    packet.mem_used_mb = wave(secondaryPhase + 1.2, 40000.0, 15000.0);
    packet.mem_pressure_pct = wave(phase + 2.0, 60.0, 12.0);
    packet.power_draw_w = wave(secondaryPhase + 2.1, 170.0, 115.0);
    packet.fan_rpm = wave(phase + 0.8, 2100.0, 1300.0);
    packet.thermal_throttle_active = packet.cpu_temp_c > 61.0 || packet.power_draw_w > 235.0;
    packet.thermal_throttle_reason = packet.thermal_throttle_active ? "thermal_guard" : "";
    packet.disk_util_pct = wave(phase + 1.8, 76.0, 6.0);
    packet.nvme_temp_c = wave(secondaryPhase + 0.3, 24.0, 33.0);
    packet.net_tx_mbps = wave(phase + 0.6, 240.0, 45.0);
    packet.net_rx_mbps = wave(secondaryPhase + 0.5, 260.0, 35.0);
    packet.net_latency_ms = wave(phase + 2.7, 14.0, 2.0);
    packet.packet_loss_pct = wave(secondaryPhase + 1.1, 0.3, 0.03);
    packet.ecc_error_count = static_cast<long>(std::fmod(static_cast<double>(sampleSeq) + std::fabs(lat), 3.0));
    packet.pcie_error_count = static_cast<long>(std::fmod(static_cast<double>(sampleSeq) + std::fabs(lon), 2.0));
    packet.health_score = std::max(0.0, 100.0 - (packet.cpu_util_pct * 0.17) - (packet.mem_pressure_pct * 0.2) - (packet.packet_loss_pct * 50.0) - (packet.thermal_throttle_active ? 15.0 : 0.0));
    packet.status_flags = packet.thermal_throttle_active ? "thermal_throttle" : "nominal";
    packet.source_vendor = "generic-oem";
    packet.source_model = "edge-node-v2";
    return packet;
}

} // namespace

json buildTelemetrySample(const std::string& continent,
                          const std::string& country,
                          const std::string& region,
                          const std::string& city,
                          double lat,
                          double lon,
                          long sampleSeq) {
    return to_json(buildTelemetryPacket(continent, country, region, city, lat, lon, sampleSeq));
}

// Thread function for each city
void pollCity(const std::string& continent, const std::string& country, 
              const std::string& region, const std::string& city, 
              double lat, double lon) {
    std::cout << "Starting poll thread for " << city << std::endl;

    long sampleSeq = 0;

    while (running.load()) {
        TelemetryPacket packet = buildTelemetryPacket(continent, country, region, city, lat, lon, ++sampleSeq);
        packet.payload_json = telemetry_packet_payload_json(packet);

        {
            std::unique_lock<std::mutex> lock(queueMutex);
            while (running.load() && telemetryQueue.size() >= MAX_QUEUE_SIZE) {
                queueCv.wait_for(lock, std::chrono::milliseconds(250));
            }

            if (!running.load()) {
                break;
            }

            telemetryQueue.push(packet);
            std::cout << "Pushed telemetry for " << city << " - CPU: "
                      << packet.cpu_temp_c << "C" << std::endl;
            lock.unlock();
            queueCv.notify_one();
        }

        std::unique_lock<std::mutex> lock(queueMutex);
        queueCv.wait_for(lock, std::chrono::seconds(5), [] {
            return !running.load();
        });
    }

}

int main() {
    std::signal(SIGINT, handleShutdownSignal);
    std::signal(SIGTERM, handleShutdownSignal);

    std::cout << "=== South India Collector ===" << std::endl;
    std::cout << "Publishing telemetry envelopes to the India regional gateway..." << std::endl;

    TcpBrokerPublisher publisher(runtimeTcpHost(), 9101);

    std::ifstream configFile("../configs/south_india.json");
    if (!configFile.is_open()) {
        std::cerr << "Failed to open config file" << std::endl;
        return 1;
    }

    json config = json::parse(configFile);
    configFile.close();

    std::string continent = config["continent"];
    std::string country = config["country"];
    std::string region = config["region"];

    std::vector<std::thread> threads;

    // Create a thread for each city
    for (const auto& cityObj : config["cities"]) {
        std::string cityName = cityObj["name"];
        double lat = cityObj["lat"];
        double lon = cityObj["lon"];

        threads.emplace_back(pollCity, continent, country, region, cityName, lat, lon);
    }

    // Create sender thread to send queued packets to aggregator
    threads.emplace_back([&publisher]() {
        while (running.load()) {
            TelemetryPacket packet;
            {
                std::unique_lock<std::mutex> lock(queueMutex);
                while (running.load() && telemetryQueue.empty()) {
                    queueCv.wait_for(lock, std::chrono::milliseconds(100));
                }

                if (!running.load()) {
                    break;
                }

                if (telemetryQueue.empty()) {
                    continue;
                }
                packet = telemetryQueue.front();
                telemetryQueue.pop();
                lock.unlock();
                queueCv.notify_one();
            }
            MessageEnvelope envelope = make_telemetry_packet_envelope(packet, "south_india_collector", "india_gateway");
            if (!publisher.publish_to_topic("south_india_events", envelope)) {
                std::cerr << "[Sender] Failed to publish packet for " << packet.city << std::endl;
            } else {
                std::cout << "[Sender] Published envelope for " << packet.city << std::endl;
            }
        }
    });

    // Keep main thread alive
    for (auto& thread : threads) {
        thread.join();
    }

    return 0;
}
