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

std::string resolveTopologyPath() {
    const std::vector<std::string> candidates = {
        "configs/global_topology.json",
        "../configs/global_topology.json"
    };

    for (const auto& candidate : candidates) {
        std::ifstream test(candidate);
        if (test.is_open()) {
            return candidate;
        }
    }

    return "configs/global_topology.json";
}

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
    const double phase = static_cast<double>(sampleSeq) * 0.31 + lat * 0.07 + lon * 0.03;
    const double secondaryPhase = phase * 1.37 + static_cast<double>(citySlug.size()) * 0.19;

    packet.node_id = region + ":" + citySlug;
    packet.asset_tag = region + "-" + citySlug;
    packet.hostname = citySlug + "." + slugify(region) + ".local";
    packet.site_id = slugify(country);
    packet.rack_id = "rack-" + std::to_string((sampleSeq % 12) + 1);
    packet.region = region;
    packet.geo_region = continent;
    packet.sample_seq = sampleSeq;
    packet.timestamp = epoch_millis_now();

    packet.cpu_temp_c = wave(phase, 26.0, 38.0);
    packet.cpu_util_pct = wave(secondaryPhase, 72.0, 18.0);
    packet.gpu_temp_c = wave(phase + 1.1, 24.0, 36.0);
    packet.gpu_util_pct = wave(secondaryPhase + 0.8, 68.0, 12.0);
    packet.vram_total_mb = 16384.0;
    packet.vram_used_mb = wave(phase + 0.5, 12000.0, 2000.0);
    packet.mem_total_mb = 65536.0;
    packet.mem_used_mb = wave(secondaryPhase + 1.2, 42000.0, 14000.0);
    packet.mem_pressure_pct = wave(phase + 2.0, 65.0, 15.0);
    packet.power_draw_w = wave(secondaryPhase + 2.2, 180.0, 110.0);
    packet.fan_rpm = wave(phase + 0.9, 2200.0, 1200.0);
    packet.thermal_throttle_active = packet.cpu_temp_c > 60.0 || packet.power_draw_w > 240.0;
    packet.thermal_throttle_reason = packet.thermal_throttle_active ? "thermal_guard" : "";
    packet.disk_util_pct = wave(phase + 1.7, 78.0, 8.0);
    packet.nvme_temp_c = wave(secondaryPhase + 0.4, 26.0, 34.0);
    packet.net_tx_mbps = wave(phase + 0.3, 260.0, 40.0);
    packet.net_rx_mbps = wave(secondaryPhase + 0.6, 280.0, 30.0);
    packet.net_latency_ms = wave(phase + 2.6, 16.0, 2.0);
    packet.packet_loss_pct = wave(secondaryPhase + 1.4, 0.35, 0.02);
    packet.ecc_error_count = static_cast<long>(std::fmod(static_cast<double>(sampleSeq) + std::fabs(lat), 3.0));
    packet.pcie_error_count = static_cast<long>(std::fmod(static_cast<double>(sampleSeq) + std::fabs(lon), 2.0));
    packet.health_score = std::max(0.0, 100.0 - (packet.cpu_util_pct * 0.16) - (packet.mem_pressure_pct * 0.22) - (packet.packet_loss_pct * 45.0) - (packet.thermal_throttle_active ? 14.0 : 0.0));
    packet.status_flags = packet.thermal_throttle_active ? "thermal_throttle" : "nominal";
    packet.source_vendor = "generic-oem";
    packet.source_model = "edge-node-v2";
    return packet;
}

} // namespace

void pollCity(const std::string& continent, const std::string& country,
              const std::string& region, const std::string& city,
              double lat, double lon) {
    std::cout << "[Collector] Starting poll thread for " << city << std::endl;

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
            std::cout << "[Collector] Queued telemetry for " << city << " - CPU: "
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

int main(int argc, char* argv[]) {
    std::signal(SIGINT, handleShutdownSignal);
    std::signal(SIGTERM, handleShutdownSignal);

    // Load topology config
    const std::string topologyPath = resolveTopologyPath();
    std::ifstream topologyFile(topologyPath);
    if (!topologyFile.is_open()) {
        std::cerr << "Failed to open topology config" << std::endl;
        return 1;
    }

    json topology = json::parse(topologyFile);
    topologyFile.close();

    // Find the region in the topology
    std::string regionName = "south_india";
    if (argc > 1) {
        regionName = argv[1];
    }

    auto regions = topology["topology"]["regions"];
    json region = nullptr;
    for (const auto& r : regions) {
        if (r["name"].get<std::string>() == regionName) {
            region = r;
            break;
        }
    }

    if (region.is_null()) {
        std::cerr << "Region '" << regionName << "' not found in topology" << std::endl;
        return 1;
    }

    std::string continent = region["continent"];
    std::string country = region["country"];

    std::cout << "=== Regional Collector: " << regionName << " ===" << std::endl;
    std::cout << "Region: " << regionName << ", Country: " << country << ", Continent: " << continent << std::endl;
    std::cout << "Publishing to broker topic: " << regionName << "_events" << std::endl;
    std::cout << std::endl;

    const int sendToPort = region["send_to_port"].get<int>();
    auto brokerPublisher = std::make_shared<TcpBrokerPublisher>(runtimeTcpHost(), sendToPort);
    std::vector<std::thread> threads;

    // Create a poll thread for each city
    for (const auto& cityObj : region["cities"]) {
        std::string cityName = cityObj["name"];
        double lat = cityObj["latitude"];
        double lon = cityObj["longitude"];

        threads.emplace_back(pollCity, continent, country, regionName, cityName, lat, lon);
    }

    // Create sender thread
    threads.emplace_back([&brokerPublisher, &regionName, sendToPort]() {
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

            MessageEnvelope envelope = make_telemetry_packet_envelope(packet, "collector:" + regionName, regionName + "_aggregator");
            if (!brokerPublisher->publish_to_topic(regionName + "_events", envelope)) {
                std::cerr << "[Sender] Failed to publish packet for " << packet.node_id << std::endl;
            } else {
                std::cout << "[Sender] Published " << packet.node_id << " to socket port " << sendToPort << std::endl;
            }
        }
    });

    // Keep main thread alive
    for (auto& thread : threads) {
        thread.join();
    }

    return 0;
}
