#pragma once

#include <atomic>
#include <chrono>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "../models/WeatherPacket.hpp"
#include "MessageTypes.hpp"

using json = nlohmann::json;

inline long epoch_millis_now() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

struct MessageEnvelope {
    int schema_version;
    std::string message_id;
    std::string source;
    std::string route;
    std::string message_type;
    long created_at;
    json payload;
};

inline std::string generate_message_id(const std::string& source) {
    static std::atomic<unsigned long> sequence{0};
    const auto now = epoch_millis_now();
    return source + "-" + std::to_string(now) + "-" + std::to_string(sequence.fetch_add(1));
}

inline MessageEnvelope make_weather_packet_envelope(const WeatherPacket& packet,
                                                    const std::string& source,
                                                    const std::string& route) {
    return {
        1,
        generate_message_id(source),
        source,
        route,
        MessageTypes::WeatherPacket,
        epoch_millis_now(),
        to_json(packet)
    };
}

inline json envelope_to_json(const MessageEnvelope& envelope) {
    return {
        {"schema_version", envelope.schema_version},
        {"message_id", envelope.message_id},
        {"source", envelope.source},
        {"route", envelope.route},
        {"message_type", envelope.message_type},
        {"created_at", envelope.created_at},
        {"payload", envelope.payload}
    };
}

inline MessageEnvelope envelope_from_json(const json& data) {
    return {
        data.at("schema_version").get<int>(),
        data.at("message_id").get<std::string>(),
        data.at("source").get<std::string>(),
        data.at("route").get<std::string>(),
        data.at("message_type").get<std::string>(),
        data.at("created_at").get<long>(),
        data.at("payload")
    };
}

inline bool is_weather_packet_envelope(const MessageEnvelope& envelope) {
    return envelope.schema_version == 1 && envelope.message_type == MessageTypes::WeatherPacket &&
           envelope.payload.is_object();
}

inline std::optional<WeatherPacket> weather_packet_from_envelope(const MessageEnvelope& envelope) {
    if (!is_weather_packet_envelope(envelope)) {
        return std::nullopt;
    }

    try {
        return weather_packet_from_json(envelope.payload);
    } catch (...) {
        return std::nullopt;
    }
}