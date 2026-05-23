#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>
#include <unordered_map>

#include "../protocol/MessageEnvelope.hpp"
#include "../pipeline/ValidationAggregationConsumerPipeline.hpp"
#include "IBrokerSubscriber.hpp"

/**
 * In-process broker implementation using thread-safe queues.
 * Useful for testing and development before integrating real Kafka/NATS.
 * 
 * Multiple publishers can push envelopes to a topic queue.
 * A single subscriber consumes from that queue and feeds the pipeline.
 * 
 * In production, this would be replaced by KafkaSubscriber or NatsJetStreamSubscriber.
 */
class InProcessBrokerSubscriber : public IBrokerSubscriber {
public:
    InProcessBrokerSubscriber(std::string topic,
                             std::shared_ptr<ValidationAggregationConsumerPipeline> pipeline,
                             std::string consumerName)
        : topics_{std::move(topic)},
          pipeline_(std::move(pipeline)),
          consumerName_(std::move(consumerName)) {}

    InProcessBrokerSubscriber(std::vector<std::string> topics,
                             std::shared_ptr<ValidationAggregationConsumerPipeline> pipeline,
                             std::string consumerName)
        : topics_(std::move(topics)),
          pipeline_(std::move(pipeline)),
          consumerName_(std::move(consumerName)) {}

    void run(std::atomic<bool>& running) override {
        std::cout << "[" << consumerName_ << "] Starting in-process broker subscriber on topic '"
                  << (topics_.empty() ? std::string("<none>") : topics_.front()) << "'" << std::endl;

        while (running.load()) {
            MessageEnvelope envelope;
            bool foundEnvelope = false;

            for (const auto& topic : topics_) {
                std::unique_lock<std::mutex> lock(queue_mutex_for_topic(topic));
                auto& queue = queue_for_topic(topic);
                if (queue.empty()) {
                    continue;
                }

                envelope = queue.front();
                queue.pop();
                foundEnvelope = true;
                lock.unlock();
                queue_cv_for_topic(topic).notify_one();
                break;
            }

            if (!foundEnvelope) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }

            // Process envelope through the pipeline
            if (!pipeline_->consume(envelope)) {
                std::cerr << "[" << consumerName_ << "] Failed to consume envelope " << envelope.message_id << std::endl;
            }
        }

        std::cout << "[" << consumerName_ << "] Stopped in-process broker subscriber" << std::endl;
    }

    /**
     * Static method to publish an envelope to a topic queue.
     * Can be called by any publisher (collector, gateway, etc.)
     */
    static void publish_to_topic(const std::string& topic, const MessageEnvelope& envelope) {
        std::unique_lock<std::mutex> lock(registry_mutex());
        auto& queue = topic_queues()[topic];
        auto& mutex = topic_mutexes()[topic];
        auto& condition = topic_cvs()[topic];
        (void)queue;
        (void)mutex;
        (void)condition;
        lock.unlock();

        std::unique_lock<std::mutex> queueLock(queue_mutex_for_topic(topic));
        queue_for_topic(topic).push(envelope);
        queueLock.unlock();
        queue_cv_for_topic(topic).notify_one();
    }

private:
    std::vector<std::string> topics_;
    std::shared_ptr<ValidationAggregationConsumerPipeline> pipeline_;
    std::string consumerName_;

    // Global storage for topic queues
    static std::unordered_map<std::string, std::queue<MessageEnvelope>>& topic_queues() {
        static std::unordered_map<std::string, std::queue<MessageEnvelope>> queues;
        return queues;
    }

    static std::unordered_map<std::string, std::mutex>& topic_mutexes() {
        static std::unordered_map<std::string, std::mutex> mutexes;
        return mutexes;
    }

    static std::unordered_map<std::string, std::condition_variable>& topic_cvs() {
        static std::unordered_map<std::string, std::condition_variable> cvs;
        return cvs;
    }

    static std::mutex& registry_mutex() {
        static std::mutex registryMutex;
        return registryMutex;
    }

    // Static accessors for publishers
    static std::queue<MessageEnvelope>& queue_for_topic(const std::string& topic) {
        return topic_queues()[topic];
    }

    static std::mutex& queue_mutex_for_topic(const std::string& topic) {
        return topic_mutexes()[topic];
    }

    static std::condition_variable& queue_cv_for_topic(const std::string& topic) {
        return topic_cvs()[topic];
    }
};
