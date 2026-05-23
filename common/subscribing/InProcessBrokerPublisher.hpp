#pragma once

#include <string>
#include "../protocol/MessageEnvelope.hpp"
#include "InProcessBrokerSubscriber.hpp"
#include "IBrokerPublisher.hpp"

/**
 * In-process broker publisher.
 * Publishes envelopes to in-memory topic queues.
 * Works with InProcessBrokerSubscriber for local testing/development.
 */
class InProcessBrokerPublisher : public IBrokerPublisher {
public:
    bool publish_to_topic(const std::string& topic, const MessageEnvelope& envelope) override {
        try {
            InProcessBrokerSubscriber::publish_to_topic(topic, envelope);
            return true;
        } catch (const std::exception& e) {
            return false;
        }
    }
};
