#ifndef TCP_SOCKET_HPP
#define TCP_SOCKET_HPP

#include <string>

class TCPSocket {
public:
    TCPSocket();
    ~TCPSocket();

    bool createServer(int port);
    int acceptClient(int timeoutMs = -1);
    bool connectToServer(const std::string& host, int port);
    bool sendMessage(const std::string& message);
    int receiveMessage(std::string& message, int timeoutMs = -1);
    void closeConnection();
    void setSocketFd(int fd);

private:
    int socketFd;
    int clientFd;
};

#endif // TCP_SOCKET_HPP
