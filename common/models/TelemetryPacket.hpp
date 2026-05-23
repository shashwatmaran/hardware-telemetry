#pragma once

#include <string>

#include <nlohmann/json.hpp>

using json = nlohmann::json;

struct TelemetryPacket {
    std::string node_id;
    std::string asset_tag;
    std::string hostname;
    std::string site_id;
    std::string rack_id;
    std::string region;
    std::string geo_region;
    long sample_seq{0};

    long timestamp{0};

    double cpu_temp_c{0.0};
    double cpu_util_pct{0.0};
    double gpu_temp_c{0.0};
    double gpu_util_pct{0.0};
    double vram_used_mb{0.0};
    double vram_total_mb{0.0};
    double mem_used_mb{0.0};
    double mem_total_mb{0.0};
    double mem_pressure_pct{0.0};
    double power_draw_w{0.0};
    double fan_rpm{0.0};
    bool thermal_throttle_active{false};
    std::string thermal_throttle_reason;
    double disk_util_pct{0.0};
    double nvme_temp_c{0.0};
    double net_tx_mbps{0.0};
    double net_rx_mbps{0.0};
    double net_latency_ms{0.0};
    double packet_loss_pct{0.0};
    long ecc_error_count{0};
    long pcie_error_count{0};
    double health_score{0.0};
    std::string status_flags;
    std::string source_vendor;
    std::string source_model;
    json payload_json;
};

inline json telemetry_packet_payload_json(const TelemetryPacket& packet) {
    return {
        {"node_id", packet.node_id},
        {"asset_tag", packet.asset_tag},
        {"hostname", packet.hostname},
        {"site_id", packet.site_id},
        {"rack_id", packet.rack_id},
        {"region", packet.region},
        {"geo_region", packet.geo_region},
        {"sample_seq", packet.sample_seq},
        {"timestamp", packet.timestamp},
        {"cpu_temp_c", packet.cpu_temp_c},
        {"cpu_util_pct", packet.cpu_util_pct},
        {"gpu_temp_c", packet.gpu_temp_c},
        {"gpu_util_pct", packet.gpu_util_pct},
        {"vram_used_mb", packet.vram_used_mb},
        {"vram_total_mb", packet.vram_total_mb},
        {"mem_used_mb", packet.mem_used_mb},
        {"mem_total_mb", packet.mem_total_mb},
        {"mem_pressure_pct", packet.mem_pressure_pct},
        {"power_draw_w", packet.power_draw_w},
        {"fan_rpm", packet.fan_rpm},
        {"thermal_throttle_active", packet.thermal_throttle_active},
        {"thermal_throttle_reason", packet.thermal_throttle_reason},
        {"disk_util_pct", packet.disk_util_pct},
        {"nvme_temp_c", packet.nvme_temp_c},
        {"net_tx_mbps", packet.net_tx_mbps},
        {"net_rx_mbps", packet.net_rx_mbps},
        {"net_latency_ms", packet.net_latency_ms},
        {"packet_loss_pct", packet.packet_loss_pct},
        {"ecc_error_count", packet.ecc_error_count},
        {"pcie_error_count", packet.pcie_error_count},
        {"health_score", packet.health_score},
        {"status_flags", packet.status_flags},
        {"source_vendor", packet.source_vendor},
        {"source_model", packet.source_model}
    };
}

inline TelemetryPacket telemetry_packet_from_json(const json& packetJson) {
    TelemetryPacket p;

    if (packetJson.contains("node_id")) p.node_id = packetJson.at("node_id").get<std::string>();
    if (packetJson.contains("asset_tag")) p.asset_tag = packetJson.at("asset_tag").get<std::string>();
    if (packetJson.contains("hostname")) p.hostname = packetJson.at("hostname").get<std::string>();
    if (packetJson.contains("site_id")) p.site_id = packetJson.at("site_id").get<std::string>();
    if (packetJson.contains("rack_id")) p.rack_id = packetJson.at("rack_id").get<std::string>();
    if (packetJson.contains("region")) p.region = packetJson.at("region").get<std::string>();
    if (packetJson.contains("geo_region")) p.geo_region = packetJson.at("geo_region").get<std::string>();
    if (packetJson.contains("sample_seq")) p.sample_seq = packetJson.at("sample_seq").get<long>();
    if (packetJson.contains("timestamp")) p.timestamp = packetJson.at("timestamp").get<long>();

    if (packetJson.contains("cpu_temp_c")) p.cpu_temp_c = packetJson.at("cpu_temp_c").get<double>();
    if (packetJson.contains("cpu_util_pct")) p.cpu_util_pct = packetJson.at("cpu_util_pct").get<double>();
    if (packetJson.contains("gpu_temp_c")) p.gpu_temp_c = packetJson.at("gpu_temp_c").get<double>();
    if (packetJson.contains("gpu_util_pct")) p.gpu_util_pct = packetJson.at("gpu_util_pct").get<double>();
    if (packetJson.contains("vram_used_mb")) p.vram_used_mb = packetJson.at("vram_used_mb").get<double>();
    if (packetJson.contains("vram_total_mb")) p.vram_total_mb = packetJson.at("vram_total_mb").get<double>();
    if (packetJson.contains("mem_used_mb")) p.mem_used_mb = packetJson.at("mem_used_mb").get<double>();
    if (packetJson.contains("mem_total_mb")) p.mem_total_mb = packetJson.at("mem_total_mb").get<double>();
    if (packetJson.contains("mem_pressure_pct")) p.mem_pressure_pct = packetJson.at("mem_pressure_pct").get<double>();
    if (packetJson.contains("power_draw_w")) p.power_draw_w = packetJson.at("power_draw_w").get<double>();
    if (packetJson.contains("fan_rpm")) p.fan_rpm = packetJson.at("fan_rpm").get<double>();
    if (packetJson.contains("thermal_throttle_active")) p.thermal_throttle_active = packetJson.at("thermal_throttle_active").get<bool>();
    if (packetJson.contains("thermal_throttle_reason")) p.thermal_throttle_reason = packetJson.at("thermal_throttle_reason").get<std::string>();
    if (packetJson.contains("disk_util_pct")) p.disk_util_pct = packetJson.at("disk_util_pct").get<double>();
    if (packetJson.contains("nvme_temp_c")) p.nvme_temp_c = packetJson.at("nvme_temp_c").get<double>();
    if (packetJson.contains("net_tx_mbps")) p.net_tx_mbps = packetJson.at("net_tx_mbps").get<double>();
    if (packetJson.contains("net_rx_mbps")) p.net_rx_mbps = packetJson.at("net_rx_mbps").get<double>();
    if (packetJson.contains("net_latency_ms")) p.net_latency_ms = packetJson.at("net_latency_ms").get<double>();
    if (packetJson.contains("packet_loss_pct")) p.packet_loss_pct = packetJson.at("packet_loss_pct").get<double>();
    if (packetJson.contains("ecc_error_count")) p.ecc_error_count = packetJson.at("ecc_error_count").get<long>();
    if (packetJson.contains("pcie_error_count")) p.pcie_error_count = packetJson.at("pcie_error_count").get<long>();
    if (packetJson.contains("health_score")) p.health_score = packetJson.at("health_score").get<double>();
    if (packetJson.contains("status_flags")) p.status_flags = packetJson.at("status_flags").get<std::string>();
    if (packetJson.contains("source_vendor")) p.source_vendor = packetJson.at("source_vendor").get<std::string>();
    if (packetJson.contains("source_model")) p.source_model = packetJson.at("source_model").get<std::string>();
    if (packetJson.contains("payload_json")) {
        p.payload_json = packetJson.at("payload_json");
    } else {
        p.payload_json = packetJson;
    }

    return p;
}

inline json to_json(const TelemetryPacket& packet) {
    json j = telemetry_packet_payload_json(packet);
    j["payload_json"] = packet.payload_json.is_null() ? telemetry_packet_payload_json(packet) : packet.payload_json;
    return j;
}