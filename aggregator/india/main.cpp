#include <iostream>
#include <atomic>
#include <csignal>
#include "../../common/gateway/RegionalGateway.hpp"
#include "../../common/utils/RuntimeConfig.hpp"
#include "../../common/publishing/BrokerPublisher.hpp"

std::atomic<bool> running{true};

void handleShutdownSignal(int) {
    running.store(false);
}

int main() {
    std::signal(SIGINT, handleShutdownSignal);
    std::signal(SIGTERM, handleShutdownSignal);

    std::cout << "=== India Regional Gateway (Port 9101) ===" << std::endl;
    std::cout << "Relaying canonical envelopes to Asia consumer..." << std::endl;

    TcpBrokerPublisher upstreamPublisher(runtimeTcpHost(), 9201);
    RegionalGateway gateway(9101, "india_gateway", upstreamPublisher);
    gateway.run(running);

    return 0;
}
