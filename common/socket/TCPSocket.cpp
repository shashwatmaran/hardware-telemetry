#include "TCPSocket.hpp"
#include <iostream>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <poll.h>

TCPSocket::TCPSocket() : socketFd(-1), clientFd(-1) {}

TCPSocket::~TCPSocket() {
    closeConnection();
}

void TCPSocket::closeConnection() {
    if (clientFd != -1) {
        close(clientFd);
        clientFd = -1;
    }
    if (socketFd != -1) {
        close(socketFd);
        socketFd = -1;
    }
}

bool TCPSocket::createServer(int port) {
    socketFd = socket(AF_INET, SOCK_STREAM, 0);
    if (socketFd < 0) {
        std::cerr << "Error creating socket" << std::endl;
        return false;
    }

    int opt = 1;
    if (setsockopt(socketFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        std::cerr << "Error setting socket options" << std::endl;
        return false;
    }

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(socketFd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "Error binding socket" << std::endl;
        return false;
    }

    if (listen(socketFd, 1) < 0) {
        std::cerr << "Error listening on socket" << std::endl;
        return false;
    }

    std::cout << "Server listening on port " << port << std::endl;
    return true;
}

int TCPSocket::acceptClient(int timeoutMs) {
    if (socketFd < 0) {
        std::cerr << "No listening socket available" << std::endl;
        return -1;
    }

    if (timeoutMs >= 0) {
        struct pollfd pfd;
        pfd.fd = socketFd;
        pfd.events = POLLIN;

        int pollResult = poll(&pfd, 1, timeoutMs);
        if (pollResult == 0) {
            return 0;
        }
        if (pollResult < 0) {
            std::cerr << "Error waiting for client connection" << std::endl;
            return -1;
        }
    }

    struct sockaddr_in clientAddr;
    socklen_t clientAddrLen = sizeof(clientAddr);

    clientFd = accept(socketFd, (struct sockaddr*)&clientAddr, &clientAddrLen);
    if (clientFd < 0) {
        std::cerr << "Error accepting client" << std::endl;
        return -1;
    }

    std::cout << "Client connected from " << inet_ntoa(clientAddr.sin_addr) << std::endl;
    return clientFd;
}

bool TCPSocket::connectToServer(const std::string& host, int port) {
    closeConnection();

    socketFd = socket(AF_INET, SOCK_STREAM, 0);
    if (socketFd < 0) {
        std::cerr << "Error creating socket" << std::endl;
        return false;
    }

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        std::cerr << "Invalid address: " << host << std::endl;
        return false;
    }

    if (connect(socketFd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "Error connecting to server" << std::endl;
        return false;
    }

    std::cout << "Connected to server at " << host << ":" << port << std::endl;
    return true;
}

bool TCPSocket::sendMessage(const std::string& message) {
    int fd = (clientFd != -1) ? clientFd : socketFd;
    if (fd < 0) {
        std::cerr << "No active connection" << std::endl;
        return false;
    }

    std::string framedMessage = message + "\n";
    ssize_t sent = send(fd, framedMessage.c_str(), framedMessage.length(), 0);
    if (sent < 0) {
        std::cerr << "Error sending message" << std::endl;
        return false;
    }

    return true;
}

int TCPSocket::receiveMessage(std::string& message, int timeoutMs) {
    int fd = (clientFd != -1) ? clientFd : socketFd;
    if (fd < 0) {
        std::cerr << "No active connection" << std::endl;
        return -1;
    }

    message.clear();
    char buffer[1];

    while (true) {
        if (timeoutMs >= 0) {
            struct pollfd pfd;
            pfd.fd = fd;
            pfd.events = POLLIN;

            int pollResult = poll(&pfd, 1, timeoutMs);
            if (pollResult == 0) {
                return 0;
            }
            if (pollResult < 0) {
                std::cerr << "Error waiting for message" << std::endl;
                return -1;
            }
        }

        ssize_t received = recv(fd, buffer, 1, 0);
        if (received < 0) {
            std::cerr << "Error receiving message" << std::endl;
            return -1;
        }
        if (received == 0) {
            std::cout << "Connection closed" << std::endl;
            return -1;
        }
        
        if (buffer[0] == '\n') {
            break;
        }
        message += buffer[0];
    }

    return 1;
}

void TCPSocket::setSocketFd(int fd) {
    socketFd = fd;
}
