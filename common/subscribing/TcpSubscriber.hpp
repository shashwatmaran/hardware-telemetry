#pragma once

#include <thread>
#include <vector>
#include <memory>
#include <atomic>
#include <iostream>

#include "../socket/TCPSocket.hpp"
#include "../pipeline/ValidationAggregationConsumerPipeline.hpp"
#include "../protocol/MessageEnvelope.hpp"
#include "IBrokerPublisher.hpp"
#include <nlohmann/json.hpp>

using json = nlohmann::json;

/**
 * TCP adapter that listens on a socket and publishes envelopes to a broker.
 * Separates the transport layer (TCP) from the consumption layer (broker/pipeline).
 */
class TcpSubscriber {
public:
    // Mode 1: Direct pipeline consumption (for simple testing)
    TcpSubscriber(int listenPort,
                  std::shared_ptr<ValidationAggregationConsumerPipeline> pipeline,
                  std::string name)
        : listenPort_(listenPort), pipeline_(std::move(pipeline)), name_(std::move(name)), brokerPublisher_(nullptr), topic_("") {}

    // Mode 2: Broker-backed (publishes to topic instead of calling pipeline)
    TcpSubscriber(int listenPort,
                  std::shared_ptr<IBrokerPublisher> brokerPublisher,
                  std::string topic,
                  std::string name)
        : listenPort_(listenPort), pipeline_(nullptr), name_(std::move(name)), brokerPublisher_(std::move(brokerPublisher)), topic_(std::move(topic)) {}

    // Runs the subscriber loop on the current thread until running becomes false.
    void run(std::atomic<bool>& running) {
        TCPSocket serverSocket;

        if (!serverSocket.createServer(listenPort_)) {
            std::cerr << "[" << name_ << "] Failed to create server on port " << listenPort_ << std::endl;
            return;
        }

        std::vector<std::thread> receiveThreads;

        while (running.load()) {
            int clientFd = serverSocket.acceptClient(1000);
            if (clientFd == 0) {
                continue;
            }
            if (clientFd < 0) {
                if (!running.load()) {
                    break;
                }
                std::cerr << "[" << name_ << "] Failed to accept client" << std::endl;
                continue;
            }

            receiveThreads.emplace_back([this, clientFd, &running]() {
                TCPSocket clientSocket;
                clientSocket.setSocketFd(clientFd);
                std::string message;

                while (running.load()) {
                    int status = clientSocket.receiveMessage(message, 1000);
                    if (status == 0) {
                        continue;
                    }
                    if (status < 0) {
                        break;
                    }

                    try {
                        MessageEnvelope envelope = envelope_from_json(json::parse(message));
                        
                        // Mode 1: Direct pipeline
                        if (pipeline_) {
                            pipeline_->consume(envelope);
                        }
                        // Mode 2: Publish to broker
                        else if (brokerPublisher_) {
                            if (!brokerPublisher_->publish_to_topic(topic_, envelope)) {
                                std::cerr << "[" << name_ << "] Failed to publish envelope " << envelope.message_id << " to topic " << topic_ << std::endl;
                            }
                        }
                    } catch (const std::exception& e) {
                        std::cerr << "[" << name_ << "] Envelope parse error: " << e.what() << std::endl;
                    }
                }
            });
        }

        for (auto& receiveThread : receiveThreads) {
            if (receiveThread.joinable()) {
                receiveThread.join();
            }
        }
    }

private:
    int listenPort_;
    std::shared_ptr<ValidationAggregationConsumerPipeline> pipeline_;
    std::string name_;
    std::shared_ptr<IBrokerPublisher> brokerPublisher_;
    std::string topic_;
};
