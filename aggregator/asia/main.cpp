#include <iostream>
#include <thread>
#include <atomic>
#include <csignal>
#include <vector>
#include <memory>
#include <chrono>
#include <nlohmann/json.hpp>
#include "../../common/pipeline/ValidationAggregationConsumerPipeline.hpp"
#include "../../common/protocol/MessageEnvelope.hpp"
#include "../../common/subscribing/TcpSubscriber.hpp"
#include "../../common/subscribing/InProcessBrokerSubscriber.hpp"
#include "../../common/subscribing/InProcessBrokerPublisher.hpp"

using json = nlohmann::json;

std::atomic<bool> running{true};

void handleShutdownSignal(int) {
    running.store(false);
}

int main() {
    std::signal(SIGINT, handleShutdownSignal);
    std::signal(SIGTERM, handleShutdownSignal);

    std::cout << "=== Asia Validation/Aggregation Consumer (Broker-backed) ===" << std::endl;
    std::cout << "TCP Adapter listening on port 9201 -> In-process broker -> Validation pipeline" << std::endl;
    std::cout << std::endl;

    // Create the broker publisher
    auto brokerPublisher = std::make_shared<InProcessBrokerPublisher>();

    // Create the validation/aggregation pipeline
    auto pipeline = std::make_shared<ValidationAggregationConsumerPipeline>("asia_consumer");

    // Create a TCP adapter that listens on 9201 and publishes to the "asia_events" topic
    TcpSubscriber tcpAdapter(9201, brokerPublisher, "asia_events", "tcp_adapter");

    // Create the broker subscriber that consumes from the "asia_events" topic
    InProcessBrokerSubscriber brokerSubscriber("asia_events", pipeline, "broker_consumer");

    std::cout << "[Main] Starting TCP adapter thread..." << std::endl;
    std::thread tcpAdapterThread([&tcpAdapter]() {
        tcpAdapter.run(running);
    });

    std::cout << "[Main] Starting broker consumer thread..." << std::endl;
    std::thread brokerConsumerThread([&brokerSubscriber]() {
        brokerSubscriber.run(running);
    });

    std::cout << "[Main] Waiting for signal to shutdown..." << std::endl;
    // Keep main thread alive until signal
    while (running.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::cout << "[Main] Shutdown signal received, waiting for threads..." << std::endl;
    tcpAdapterThread.join();
    brokerConsumerThread.join();

    return 0;
}
