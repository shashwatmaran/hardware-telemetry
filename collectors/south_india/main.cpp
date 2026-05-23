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

void handleShutdownSignal(int) {
    running.store(false);
}

// Callback for CURL to write response
static size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* userp) {
    userp->append((char*)contents, size * nmemb);
    return size * nmemb;
}

// Fetch weather data from Open-Meteo API
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
        std::cout << "[API] Fetched weather data successfully" << std::endl;
        return result;
    } catch (const std::exception& e) {
        std::cerr << "[API] JSON parse error: " << e.what() << std::endl;
        std::cerr << "[API] Response: " << readBuffer << std::endl;
        return json();
    }
}

// Thread function for each city
void pollCity(const std::string& continent, const std::string& country, 
              const std::string& region, const std::string& city, 
              double lat, double lon) {
    std::cout << "Starting poll thread for " << city << std::endl;

    CURL* curl = curl_easy_init();
    if (!curl) {
        std::cerr << "Failed to initialize CURL for " << city << std::endl;
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
                std::cout << "Pushed weather data for " << city << " - Temp: " 
                          << packet.temperature << "°C" << std::endl;
                lock.unlock();
                queueCv.notify_one();
            }
        } else {
            std::cerr << "Failed to fetch weather data for " << city << std::endl;
        }

        std::unique_lock<std::mutex> lock(queueMutex);
        queueCv.wait_for(lock, std::chrono::seconds(5), [] {
            return !running.load();
        });
    }

    curl_easy_cleanup(curl);
}

int main() {
    std::signal(SIGINT, handleShutdownSignal);
    std::signal(SIGTERM, handleShutdownSignal);

    if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK) {
        std::cerr << "Failed to initialize libcurl globally" << std::endl;
        return 1;
    }

    std::cout << "=== South India Collector ===" << std::endl;
    std::cout << "Publishing canonical envelopes to the India regional gateway..." << std::endl;

    TcpBrokerPublisher publisher(runtimeTcpHost(), 9101);

    std::ifstream configFile("../configs/south_india.json");
    if (!configFile.is_open()) {
        std::cerr << "Failed to open config file" << std::endl;
        curl_global_cleanup();
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

            MessageEnvelope envelope = make_weather_packet_envelope(packet, "south_india_collector", "india_gateway");
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

    curl_global_cleanup();

    return 0;
}
