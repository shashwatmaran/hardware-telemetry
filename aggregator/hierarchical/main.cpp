#include <iostream>
#include <atomic>
#include <csignal>
#include <memory>
#include <thread>
#include <chrono>
#include <fstream>
#include <vector>
#include <nlohmann/json.hpp>
#include "../../common/aggregation/BrokerAggregator.hpp"
#include "../../common/pipeline/ValidationAggregationConsumerPipeline.hpp"
#include "../../common/publishing/BrokerPublisher.hpp"
#include "../../common/subscribing/InProcessBrokerSubscriber.hpp"
#include "../../common/subscribing/TcpSubscriber.hpp"
#include "../../common/utils/RuntimeConfig.hpp"
using json = nlohmann::json;

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

int main(int argc, char* argv[]) {
    std::signal(SIGINT, handleShutdownSignal);
    std::signal(SIGTERM, handleShutdownSignal);

    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <aggregator_name>" << std::endl;
        std::cerr << "Example: " << argv[0] << " asia_regional" << std::endl;
        return 1;
    }

    std::string aggregatorName = argv[1];

    // Load topology config
    const std::string topologyPath = resolveTopologyPath();
    std::ifstream topologyFile(topologyPath);
    if (!topologyFile.is_open()) {
        std::cerr << "Failed to open topology config" << std::endl;
        return 1;
    }

    json topology = json::parse(topologyFile);
    topologyFile.close();

    auto& hierarchyConfig = topology["topology"]["hierarchy"];

    // Find the aggregator in the config
    json aggregatorConfig = nullptr;

    // Check regional aggregators
    for (const auto& agg : hierarchyConfig["regional_aggregators"]) {
        if (agg["name"].get<std::string>() == aggregatorName) {
            aggregatorConfig = agg;
            break;
        }
    }

    // Check continent aggregators
    if (aggregatorConfig.is_null()) {
        for (const auto& agg : hierarchyConfig["continent_aggregators"]) {
            if (agg["name"].get<std::string>() == aggregatorName) {
                aggregatorConfig = agg;
                break;
            }
        }
    }

    // Check global aggregator
    if (aggregatorConfig.is_null()) {
        if (hierarchyConfig["global_aggregator"]["name"].get<std::string>() == aggregatorName) {
            aggregatorConfig = hierarchyConfig["global_aggregator"];
        }
    }

    if (aggregatorConfig.is_null()) {
        std::cerr << "Aggregator '" << aggregatorName << "' not found in topology config" << std::endl;
        return 1;
    }

    // Extract config
    std::vector<std::string> consumeTopics;
    for (const auto& topic : aggregatorConfig["consumes_from"]) {
        consumeTopics.push_back(topic.get<std::string>());
    }
    std::string publishTopic = aggregatorConfig["publishes_to"].get<std::string>();
    int listenPort = aggregatorConfig["listen_port"].get<int>();
    std::string tier = aggregatorConfig["tier"].get<std::string>();

    std::cout << "=== Hierarchical Aggregator: " << aggregatorName << " ===" << std::endl;
    std::cout << "Tier: " << tier << std::endl;
    std::cout << "Listen Port: " << listenPort << std::endl;
    std::cout << std::endl;

    const int forwardPort = aggregatorConfig["forward_to_port"].get<int>();
    auto downstreamPublisher = std::make_shared<TcpBrokerPublisher>(runtimeTcpHost(), forwardPort);

    std::shared_ptr<ValidationAggregationConsumerPipeline> pipeline;
    if (tier == "global") {
        pipeline = std::make_shared<ValidationAggregationConsumerPipeline>(
            aggregatorName,
            [downstreamPublisher](const MessageEnvelope& envelope) {
                if (!downstreamPublisher->publish_to_topic("timescale_raw", envelope)) {
                    std::cerr << "[global_sink] Failed to forward envelope " << envelope.message_id
                              << " to Timescale writer" << std::endl;
                }
            }
        );
    } else {
        pipeline = std::make_shared<ValidationAggregationConsumerPipeline>(
            aggregatorName,
            [downstreamPublisher, publishTopic](const MessageEnvelope& envelope) {
                if (!downstreamPublisher->publish_to_topic(publishTopic, envelope)) {
                    std::cerr << "[Aggregator] Failed to forward envelope " << envelope.message_id
                              << " to next tier" << std::endl;
                }
            }
        );
    }

    TcpSubscriber subscriber(listenPort, pipeline, aggregatorName);

    std::cout << "[Main] Starting socket subscriber on port " << listenPort << "..." << std::endl;
    subscriber.run(running);

    return 0;
}
