#pragma once

#include <atomic>
#include <iostream>
#include <thread>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "../protocol/MessageEnvelope.hpp"
#include "../publishing/BrokerPublisher.hpp"
#include "../socket/TCPSocket.hpp"

using json = nlohmann::json;

class RegionalGateway {
public:
    RegionalGateway(int listenPort, std::string gatewayName, IBrokerPublisher& upstreamPublisher)
        : listenPort_(listenPort), gatewayName_(std::move(gatewayName)), upstreamPublisher_(upstreamPublisher) {}

    void run(std::atomic<bool>& running) {
        TCPSocket serverSocket;

        if (!serverSocket.createServer(listenPort_)) {
            std::cerr << "[" << gatewayName_ << "] Failed to create server on port " << listenPort_ << std::endl;
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
                std::cerr << "[" << gatewayName_ << "] Failed to accept client" << std::endl;
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
                        if (!is_weather_packet_envelope(envelope)) {
                            std::cerr << "[" << gatewayName_ << "] Rejected non-weather envelope " << envelope.message_id << std::endl;
                            continue;
                        }

                        if (!upstreamPublisher_.publish_to_topic("forward", envelope)) {
                            std::cerr << "[" << gatewayName_ << "] Failed to forward envelope " << envelope.message_id << std::endl;
                            continue;
                        }

                        std::cout << "[" << gatewayName_ << "] Forwarded envelope " << envelope.message_id << std::endl;
                    } catch (const std::exception& e) {
                        std::cerr << "[" << gatewayName_ << "] Envelope parse error: " << e.what() << std::endl;
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
    std::string gatewayName_;
    IBrokerPublisher& upstreamPublisher_;
};