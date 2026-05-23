#pragma once

#include <string>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

struct WeatherPacket {
    std::string continent;
    std::string country;
    std::string region;
    std::string city;

    double temperature;
    double humidity;
    double wind_speed;

    long timestamp;
};

inline WeatherPacket weather_packet_from_json(const json& packetJson) {
    return {
        packetJson.at("continent").get<std::string>(),
        packetJson.at("country").get<std::string>(),
        packetJson.at("region").get<std::string>(),
        packetJson.at("city").get<std::string>(),
        packetJson.at("temperature").get<double>(),
        packetJson.at("humidity").get<double>(),
        packetJson.at("wind_speed").get<double>(),
        packetJson.at("timestamp").get<long>()
    };
}

inline json to_json(const WeatherPacket& packet) {
    return {
        {"continent", packet.continent},
        {"country", packet.country},
        {"region", packet.region},
        {"city", packet.city},
        {"temperature", packet.temperature},
        {"humidity", packet.humidity},
        {"wind_speed", packet.wind_speed},
        {"timestamp", packet.timestamp}
    };
}