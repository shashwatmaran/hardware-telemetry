#pragma once

#include <cstdlib>
#include <string>

inline std::string runtimeTcpHost() {
    if (const char* host = std::getenv("WEATHER_RTOS_HOST")) {
        if (*host != '\0') {
            return host;
        }
    }

    return "127.0.0.1";
}