#include <iostream>
#include <unistd.h>
#include <arpa/inet.h>
#include <cstring>
#include <thread>

#include <curl/curl.h>
#include <nlohmann/json.hpp>    

using json = nlohmann::json;

#define PORT 9002

size_t WriteCallback(void* contents, size_t size,
                     size_t nmemb, std::string* output) {
    output->append((char*)contents, size * nmemb);
    return size * nmemb;
}

std::string fetchWeather() {
    CURL* curl;
    CURLcode res;

    std::string response;

    curl = curl_easy_init();

    if (curl) {
        std::string url =
            "https://api.open-meteo.com/v1/forecast?"
            "latitude=13.0827&longitude=80.2707&"
            "current=temperature_2m,relative_humidity_2m,wind_speed_10m";

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

        res = curl_easy_perform(curl);

        if (res != CURLE_OK) {
            std::cerr << "curl failed\n";
        }

        curl_easy_cleanup(curl);
    }

    return response;
}

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

    while (true) {
        std::string apiResponse = fetchWeather();

        auto data = json::parse(apiResponse);

        json packet;

        packet["city"] = "Chennai";
        packet["temperature"] =
            data["current"]["temperature_2m"];

        packet["humidity"] =
            data["current"]["relative_humidity_2m"];

        packet["wind_speed"] =
            data["current"]["wind_speed_10m"];

        std::string message = packet.dump();

        send(sock, message.c_str(), message.size(), 0);

        std::cout << "Sent: " << message << std::endl;

        sleep(5);
    }

    close(sock);

    return 0;
}