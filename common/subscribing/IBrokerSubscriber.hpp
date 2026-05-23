#pragma once

#include <atomic>
#include <memory>
#include <string>
#include "../pipeline/ValidationAggregationConsumerPipeline.hpp"

/**
 * Abstract broker subscriber interface.
 * Implementations consume from Kafka, NATS JetStream, or in-process brokers.
 * All implementations run the same pipeline and expose the same behavior.
 */
class IBrokerSubscriber {
public:
    virtual ~IBrokerSubscriber() = default;

    /**
     * Run the subscriber loop until running becomes false.
     * This is a blocking call that handles connection, reconnect, and message delivery.
     */
    virtual void run(std::atomic<bool>& running) = 0;
};
