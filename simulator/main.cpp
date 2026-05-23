#include <arpa/inet.h>
#include <cmath>
#include <cctype>
#include <cstring>
#include <iostream>
#include <thread>
#include <unistd.h>
#include <chrono>

#include <nlohmann/json.hpp>

#include "../common/models/TelemetryPacket.hpp"

using json = nlohmann::json;

#define PORT 9002

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

TelemetryPacket buildTelemetryPacket(long sampleSeq) {
    TelemetryPacket packet;
    const std::string region = "south_india";
    const std::string city = "Bangalore";
    const double lat = 13.0827;
    const double lon = 80.2707;
    const double phase = static_cast<double>(sampleSeq) * 0.31 + lat * 0.07 + lon * 0.03;
    const double secondaryPhase = phase * 1.37 + static_cast<double>(city.size()) * 0.19;

    packet.node_id = region + ":" + slugify(city);
    packet.asset_tag = region + "-" + slugify(city);
    packet.hostname = "sim-host-01";
    packet.site_id = "sim-site";
    packet.rack_id = "rack-1";
    packet.region = region;
    packet.geo_region = "Asia";
    packet.sample_seq = sampleSeq;
    packet.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

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
    packet.ecc_error_count = static_cast<long>(std::fmod(static_cast<double>(sampleSeq), 3.0));
    packet.pcie_error_count = static_cast<long>(std::fmod(static_cast<double>(sampleSeq), 2.0));
    packet.health_score = std::max(0.0, 100.0 - (packet.cpu_util_pct * 0.16) - (packet.mem_pressure_pct * 0.22) - (packet.packet_loss_pct * 45.0) - (packet.thermal_throttle_active ? 14.0 : 0.0));
    packet.status_flags = packet.thermal_throttle_active ? "thermal_throttle" : "nominal";
    packet.source_vendor = "generic-oem";
    packet.source_model = "sim-node-v2";
    return packet;
}

json buildPacketJson(long sampleSeq) {
    return to_json(buildTelemetryPacket(sampleSeq));
}

} // namespace

int main() {
    int sock = 0;
    struct sockaddr_in serv_addr;

    sock = socket(AF_INET, SOCK_STREAM, 0);

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);

    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

    connect(sock, (struct sockaddr*)&serv_addr,
            sizeof(serv_addr));

    std::cout << "Connected to aggregator\n";

    long sampleSeq = 0;

    while (true) {
        json packet = buildPacketJson(++sampleSeq);
        std::string message = packet.dump();

        send(sock, message.c_str(), message.size(), 0);

        std::cout << "Sent: " << message << std::endl;

        sleep(5);
    }

    close(sock);

    return 0;
}