#pragma once

#include <string>
#include "../protocol/MessageEnvelope.hpp"

/**
 * Abstract broker publisher interface.
 * Implementations can publish to Kafka, NATS, or in-process brokers.
 */
class IBrokerPublisher {
public:
    virtual ~IBrokerPublisher() = default;

    /**
     * Publish an envelope to a topic.
     * Returns true if successful, false on error.
     */
    virtual bool publish_to_topic(const std::string& topic, const MessageEnvelope& envelope) = 0;
};
