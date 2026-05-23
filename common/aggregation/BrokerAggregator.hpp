#pragma once

#include <atomic>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "../pipeline/ValidationAggregationConsumerPipeline.hpp"
#include "../subscribing/IBrokerSubscriber.hpp"
#include "../subscribing/IBrokerPublisher.hpp"

/**
 * Tier-agnostic aggregator component.
 * 
 * Consumes from one or more input topics, runs them through a validation/aggregation pipeline,
 * and publishes the output to an upstream topic.
 * 
 * Works at any tier: regional, country, continent, or global.
 */
class BrokerAggregator {
public:
    BrokerAggregator(std::string name,
                     std::vector<std::string> consumeTopics,
                     std::string publishTopic,
                     std::shared_ptr<IBrokerSubscriber> subscriber,
                     std::shared_ptr<IBrokerPublisher> publisher)
        : name_(std::move(name)),
          consumeTopics_(std::move(consumeTopics)),
          publishTopic_(std::move(publishTopic)),
          subscriber_(std::move(subscriber)),
          publisher_(std::move(publisher)) {}

    /**
     * Run the aggregator loop until running becomes false.
     * This spawns the subscriber on the current thread.
     */
    void run(std::atomic<bool>& running) {
        std::cout << "[" << name_ << "] Starting aggregator" << std::endl;
        std::cout << "[" << name_ << "] Consumes from: ";
        for (const auto& topic : consumeTopics_) {
            std::cout << topic << " ";
        }
        std::cout << std::endl;
        std::cout << "[" << name_ << "] Publishes to: " << publishTopic_ << std::endl;
        std::cout << std::endl;

        // Run the subscriber loop, which will consume and pass to the pipeline
        subscriber_->run(running);

        std::cout << "[" << name_ << "] Stopped aggregator" << std::endl;
    }

    const std::string& getName() const { return name_; }
    const std::vector<std::string>& getConsumeTopics() const { return consumeTopics_; }
    const std::string& getPublishTopic() const { return publishTopic_; }

private:
    std::string name_;
    std::vector<std::string> consumeTopics_;
    std::string publishTopic_;
    std::shared_ptr<IBrokerSubscriber> subscriber_;
    std::shared_ptr<IBrokerPublisher> publisher_;
};
