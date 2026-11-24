//
// Created by white on 26. 9. 29..
//

#ifndef IOCPSTUDY_SOCKETINFO_H
#define IOCPSTUDY_SOCKETINFO_H
#include <atomic>
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

// lock-free MPSC 큐 (Dmitry Vyukov 방식)
// 넣기(push)는 여러 스레드가 동시에 해도 되고, 꺼내기(pop)는 한 번에 한 스레드만 한다.
// 꺼내는 스레드는 SocketInfo::pendingCount로 정해진 "송신 담당" 하나뿐이다.
class SendQueue {
    struct Node {
        std::atomic<Node*> next{nullptr};
        SendContext* ctx = nullptr;
    };
    std::atomic<Node*> head; // 넣는 쪽 (마지막 노드)
    Node* tail;              // 꺼내는 쪽 (더미 노드)
public:
    SendQueue() { tail = new Node(); head.store(tail); }
    ~SendQueue() { while (pop()) {} delete tail; }

    void push(SendContext* ctx) {
        Node* n = new Node();
        n->ctx = ctx;
        Node* prev = head.exchange(n, std::memory_order_acq_rel);
        prev->next.store(n, std::memory_order_release);
    }
    // 비었거나, 넣는 중인 노드가 아직 연결되지 않았으면 nullptr
    SendContext* pop() {
        Node* next = tail->next.load(std::memory_order_acquire);
        if (!next) return nullptr;
        SendContext* ctx = next->ctx;
        delete tail;
        tail = next;
        return ctx;
    }
};

struct SocketInfo {
    SOCKET sock;
    RecvContext recvContext;

    SendContextIO sendContextIO;

    SendQueue sendQueue;
    SendContext* inflight = nullptr;      // 지금 WSASend 중인 메시지 (송신 담당만 건드림)
    std::atomic<long> pendingCount = 0;   // 큐 + 송신 중 메시지 수. 0→1로 만든 스레드가 송신 담당
    std::atomic<bool> closing = false;
    std::atomic<long> refCount = 1;

    char recvBuffer[max_buffer];
    DWORD recvByteLen;
    std::string userName;
};


#endif //IOCPSTUDY_SOCKETINFO_H
