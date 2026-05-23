#include <iostream>
#include <fstream>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <atomic>
#include <csignal>
#include <vector>
#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include "../../common/models/WeatherPacket.hpp"
#include "../../common/utils/RuntimeConfig.hpp"
#include "../../common/protocol/MessageEnvelope.hpp"
#include "../../common/publishing/BrokerPublisher.hpp"

using json = nlohmann::json;

constexpr std::size_t MAX_QUEUE_SIZE = 100;

std::queue<WeatherPacket> weatherQueue;
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

static size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* userp) {
    userp->append((char*)contents, size * nmemb);
    return size * nmemb;
}

json fetchWeatherData(CURL* curl, double lat, double lon) {
    std::string readBuffer;
    
    if (!curl) {
        std::cerr << "Failed to initialize CURL" << std::endl;
        return json();
    }

    std::string url = "https://api.open-meteo.com/v1/forecast?latitude=" + std::to_string(lat) +
                      "&longitude=" + std::to_string(lon) + "&current=temperature_2m,relative_humidity_2m,wind_speed_10m";

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
        std::cerr << "CURL request failed: " << curl_easy_strerror(res) << std::endl;
        return json();
    }

    try {
        json result = json::parse(readBuffer);
        return result;
    } catch (const std::exception& e) {
        std::cerr << "JSON parse error: " << e.what() << std::endl;
        return json();
    }
}

void pollCity(const std::string& continent, const std::string& country, 
              const std::string& region, const std::string& city, 
              double lat, double lon) {
    std::cout << "[Collector] Starting poll thread for " << city << std::endl;

    CURL* curl = curl_easy_init();
    if (!curl) {
        std::cerr << "[Collector] Failed to initialize CURL for " << city << std::endl;
        return;
    }

    while (running.load()) {
        json weatherData = fetchWeatherData(curl, lat, lon);
        if (!running.load()) {
            break;
        }
        
        if (!weatherData.empty() && weatherData.contains("current")) {
            WeatherPacket packet;
            packet.continent = continent;
            packet.country = country;
            packet.region = region;
            packet.city = city;
            packet.temperature = weatherData["current"]["temperature_2m"];
            packet.humidity = weatherData["current"]["relative_humidity_2m"];
            packet.wind_speed = weatherData["current"]["wind_speed_10m"];
            packet.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

            {
                std::unique_lock<std::mutex> lock(queueMutex);
                while (running.load() && weatherQueue.size() >= MAX_QUEUE_SIZE) {
                    queueCv.wait_for(lock, std::chrono::milliseconds(250));
                }

                if (!running.load()) {
                    break;
                }

                weatherQueue.push(packet);
                std::cout << "[Collector] Queued " << city << " - Temp: " 
                          << packet.temperature << "°C" << std::endl;
                lock.unlock();
                queueCv.notify_one();
            }
        } else {
            std::cerr << "[Collector] Failed to fetch weather for " << city << std::endl;
        }

        std::unique_lock<std::mutex> lock(queueMutex);
        queueCv.wait_for(lock, std::chrono::seconds(5), [] {
            return !running.load();
        });
    }

    curl_easy_cleanup(curl);
}

int main(int argc, char* argv[]) {
    std::signal(SIGINT, handleShutdownSignal);
    std::signal(SIGTERM, handleShutdownSignal);

    if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK) {
        std::cerr << "Failed to initialize libcurl" << std::endl;
        return 1;
    }

    // Load topology config
    const std::string topologyPath = resolveTopologyPath();
    std::ifstream topologyFile(topologyPath);
    if (!topologyFile.is_open()) {
        std::cerr << "Failed to open topology config" << std::endl;
        curl_global_cleanup();
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
        curl_global_cleanup();
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
            WeatherPacket packet;
            {
                std::unique_lock<std::mutex> lock(queueMutex);
                while (running.load() && weatherQueue.empty()) {
                    queueCv.wait_for(lock, std::chrono::milliseconds(100));
                }

                if (!running.load()) {
                    break;
                }

                if (weatherQueue.empty()) {
                    continue;
                }
                packet = weatherQueue.front();
                weatherQueue.pop();
                lock.unlock();
                queueCv.notify_one();
            }

            MessageEnvelope envelope = make_weather_packet_envelope(packet, "collector:" + regionName, regionName + "_aggregator");
            if (!brokerPublisher->publish_to_topic(regionName + "_events", envelope)) {
                std::cerr << "[Sender] Failed to publish packet for " << packet.city << std::endl;
            } else {
                std::cout << "[Sender] Published " << packet.city << " to socket port " << sendToPort << std::endl;
            }
        }
    });

    // Keep main thread alive
    for (auto& thread : threads) {
        thread.join();
    }

    curl_global_cleanup();
    return 0;
}
