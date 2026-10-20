//
// Created by white on 26. 9. 29..
//

#ifndef IOCPSTUDY_SOCKETINFO_H
#define IOCPSTUDY_SOCKETINFO_H
#include <mutex>
#include <queue>
#include <string>
#include <WinSock2.h>
#define max_buffer 1024
#define PORT 8080
#define TYPE_SEND 1
#define TYPE_RECV 2
enum class IOType{Send,Recv};
struct RecvContext {
    WSAOVERLAPPED overlapped;
    WSABUF recvBuf;
};

struct SendContext {
    char buffer[max_buffer];
    DWORD length;
    std::atomic<long> refCount = 1;
};
struct SendContextIO {
    WSAOVERLAPPED overlapped;
    WSABUF sendBuf;
};

struct SocketInfo {
    SOCKET sock;
    RecvContext recvContext;

    SendContextIO sendContextIO;

    std::queue<SendContext*> sendQueue;
    std::mutex sendMutex;
    bool isSending = false;
    bool closing = false;
    std::atomic<long> refCount = 1;

    char recvBuffer[max_buffer];
    DWORD recvByteLen;
    std::string userName;
};


#endif //IOCPSTUDY_SOCKETINFO_H