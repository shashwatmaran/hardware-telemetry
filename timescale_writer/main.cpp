#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <thread>

#include <nlohmann/json.hpp>

#include "../common/pipeline/ValidationAggregationConsumerPipeline.hpp"
#include "../common/socket/TCPSocket.hpp"
#include "../common/subscribing/TcpSubscriber.hpp"
#include "../common/timescale/AsyncQueueWriter.hpp"

using json = nlohmann::json;

std::atomic<bool> running{true};

void handleShutdownSignal(int) {
    running.store(false);
}

int main() {
    std::signal(SIGINT, handleShutdownSignal);
    std::signal(SIGTERM, handleShutdownSignal);

    std::cout << "=== Timescale Writer (Tier 2: Async Queue) ===" << std::endl;
    std::cout << "Queue-based architecture with region partitioning" << std::endl;
    std::cout << "Auto-detecting local TimescaleDB..." << std::endl;

    // Configure Tier 2: bounded queue + region partitioning
    AsyncQueueWriter::Config queueConfig;
    queueConfig.maxQueueSize = 10240;      // 10K messages
    queueConfig.batchSize = 500;           // Batch every 500
    queueConfig.flushInterval = std::chrono::milliseconds(1000);
    queueConfig.outboxPath = "timescale_outbox.sql";
    queueConfig.maxRetries = 3;
    queueConfig.retryDelay = std::chrono::milliseconds(100);

    auto writer = std::make_shared<AsyncQueueWriter>(queueConfig);

    auto pipeline = std::make_shared<ValidationAggregationConsumerPipeline>(
        "timescale_writer",
        [writer](const MessageEnvelope& envelope) {
            if (!writer->submit(envelope)) {
                std::cerr << "[timescale_writer] Queue full for " << envelope.message_id << std::endl;
            }
        }
    );

    // Start socket subscriber on a separate thread (receives data)
    std::thread subscriberThread([&pipeline]() {
        TcpSubscriber subscriber(13101, pipeline, "timescale_writer_tcp");
        subscriber.run(running);
    });

    // Run writer on main thread (manages queue + batch flushing)
    writer->run(running);

    // Wait for subscriber thread to finish (it waits for running signal)
    if (subscriberThread.joinable()) {
        subscriberThread.join();
    }

    // Print final metrics
    const auto& metrics = writer->getMetrics();
    std::cout << "\n=== Final Statistics ===" << std::endl;
    std::cout << "Messages processed: " << metrics.messagesProcessed << std::endl;
    std::cout << "Batches flushed: " << metrics.batchesFlushed << std::endl;
    std::cout << "Write errors: " << metrics.writeErrors << std::endl;
    std::cout << "Backpressure events: " << metrics.backpressureEvents << std::endl;
    std::cout << "Final queue depth: " << writer->getQueueDepth() << std::endl;

    return 0;
}
