#pragma once

#include <string>
#include <utility>

#include "../protocol/MessageEnvelope.hpp"
#include "../socket/TCPSocket.hpp"
#include "../subscribing/IBrokerPublisher.hpp"

/**
 * TCP-based broker publisher.
 * Connects to a TCP server and publishes envelopes.
 * For point-to-point communication; use real broker for multi-consumer scenarios.
 */
class TcpBrokerPublisher final : public IBrokerPublisher {
public:
    TcpBrokerPublisher(std::string host, int port)
        : host_(std::move(host)), port_(port) {}

    bool publish_to_topic(const std::string& topic, const MessageEnvelope& envelope) override {
        // TCP publisher ignores topic and sends directly to the configured host:port
        // The receiving side is responsible for routing based on the envelope
        if (!connected_) {
            connected_ = socket_.connectToServer(host_, port_);
        }

        if (!connected_) {
            return false;
        }

        if (!socket_.sendMessage(envelope_to_json(envelope).dump())) {
            socket_.closeConnection();
            connected_ = false;
            return false;
        }

        return true;
    }

private:
    std::string host_;
    int port_;
    TCPSocket socket_;
    bool connected_{false};
};