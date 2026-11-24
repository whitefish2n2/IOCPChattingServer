//
// Created by white on 26. 9. 16..
//
#include "Iocp.h"

#include <iostream>
#include <iso646.h>
#include <ostream>
#include <thread>
#include <utility>

#include "Service.h"
#include "SocketInfo.h"

using namespace std;


bool Iocp::Initialize() {
    WSADATA wsaData;
    int nResult;
    nResult = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (nResult != 0) {
        cout << "WSAStartup failed with error: " << nResult << endl;
        return false;
    }

    listenSocket = WSASocket(AF_INET,SOCK_STREAM, 0, NULL, 0, WSA_FLAG_OVERLAPPED);
    if (listenSocket == INVALID_SOCKET) {
        cout << "WSAStartup failed with error: " << WSAGetLastError() << endl;
        WSACleanup();
        return false;
    }

    SOCKADDR_IN serverAddress;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(PORT);
    serverAddress.sin_addr.s_addr = htonl(INADDR_ANY);

    nResult = bind(listenSocket, (SOCKADDR*)&serverAddress, sizeof(SOCKADDR_IN));
    if (nResult == SOCKET_ERROR) {
        cout << "bind failed with error: " << WSAGetLastError() << endl;
        closesocket(listenSocket);
        WSACleanup();
        return false;
    }

    nResult = listen(listenSocket, SOMAXCONN);
    if (nResult == SOCKET_ERROR) {
        cout << "listen failed with error: " << WSAGetLastError() << endl;
        closesocket(listenSocket);
        WSACleanup();
        return false;
    }

    return true;
}

void Iocp::StartServer() {
    int nResult;
    SOCKADDR_IN clientAddr;
    int addrLen = sizeof(clientAddr);
    SOCKET clientSocket;
    DWORD recvSize;
    DWORD flags;

    iocpHandle = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);

    if (!CreateWorker()) return;

    cout<<"Started!!"<<endl;

    while (Accepted) {
        clientSocket = WSAAccept(listenSocket, (struct sockaddr* )&clientAddr, &addrLen,NULL,NULL);
        if (clientSocket == INVALID_SOCKET) {
            cout << "WSAAccept failed with error: " << WSAGetLastError() << endl;
            continue;
        }
        m_pSocketInfo = new SocketInfo();
        m_pSocketInfo->sock = clientSocket;
        m_pSocketInfo->recvByteLen = 0;
        m_pSocketInfo->recvContext.recvBuf.len = max_buffer;
        m_pSocketInfo->recvContext.recvBuf.buf = m_pSocketInfo->recvBuffer;
        flags = 0;
        if (!CreateIoCompletionPort((HANDLE)clientSocket, iocpHandle, (ULONG_PTR)m_pSocketInfo, 0)) {
            cout << "CreateIoCompletionPort failed with error: " << WSAGetLastError() << endl;
            closesocket(clientSocket);
            delete m_pSocketInfo;
            continue;
        }

        nResult = WSARecv(m_pSocketInfo->sock, &m_pSocketInfo->recvContext.recvBuf, 1, &recvSize, &flags, &(m_pSocketInfo->recvContext.overlapped), NULL);

        if (nResult == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING) {
            cout << "WSARecv failed with error: " << WSAGetLastError() << endl;
            closesocket(m_pSocketInfo->sock);
            delete m_pSocketInfo;
            continue;
        }
    }
}


void ProcessPacket(SocketInfo* socketInfo, char* packet, WORD size);
void Iocp::WorkerThread() {
    bool bResult;
    int nResult;

    DWORD recvSize;
    DWORD sendBytes;

    SocketInfo * completionKey;
    LPOVERLAPPED overlapped;
    DWORD flags = 0;

    while (workerThreadRunning) {
        bResult = GetQueuedCompletionStatus(iocpHandle, &recvSize, (PULONG_PTR)&completionKey, &overlapped,INFINITE);
        if (overlapped == NULL) break;
        if (!bResult || recvSize == 0) {
            cout << "접속 끊김 : " << endl;
            Disconnect(completionKey);
            if (overlapped != &(completionKey->recvContext.overlapped)) {
                // 송신 실패: 보내던 메시지만 놓는다. pendingCount는 줄이지 않아 새 송신 담당이 생기지 않고,
                // 큐에 남은 것은 ReleaseIoRef에서 정리한다
                SendContext* context = completionKey->inflight;
                completionKey->inflight = nullptr;
                if (context && context->refCount.fetch_sub(1) == 1) delete context;
            }
            ReleaseIoRef(completionKey);
            continue;
        }

        if (overlapped == &(completionKey->recvContext.overlapped)) {
            completionKey->recvByteLen += recvSize;
            while (completionKey->recvByteLen >= sizeof(packetHeader)) {
                packetHeader * pHeader = (packetHeader *)completionKey->recvBuffer;

                DWORD len = pHeader->messageLength;
                //cout<<"메시지 수신, 길이: "<< recvSize<<endl;
                if (len < sizeof(packetHeader) || len > max_buffer) { Disconnect(completionKey);break; }
                if (completionKey->recvByteLen < len) {
                    break;
                };
                ProcessPacket(completionKey, completionKey->recvBuffer, len);
                completionKey->recvByteLen -= len;
                memmove(completionKey->recvBuffer, completionKey->recvBuffer + pHeader->messageLength, completionKey->recvByteLen);
            }
            bool failed = false;
            if (!completionKey->closing) {
                completionKey->recvContext.recvBuf.buf = completionKey->recvBuffer + completionKey->recvByteLen;
                completionKey->recvContext.recvBuf.len = max_buffer - completionKey->recvByteLen;
                ZeroMemory(&completionKey->recvContext.overlapped, sizeof(OVERLAPPED));
                completionKey->refCount.fetch_add(1);
                int nResult = WSARecv(completionKey->sock, &completionKey->recvContext.recvBuf, 1, &recvSize, &flags, &completionKey->recvContext.overlapped, NULL);
                if (nResult == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING) {
                    completionKey->refCount.fetch_sub(1);
                    failed = true;
                }
                // 락 대신: 거는 사이 Disconnect가 CancelIoEx를 먼저 했으면 여기서 직접 취소한다
                else if (completionKey->closing) {
                    CancelIoEx((HANDLE)completionKey->sock, &completionKey->recvContext.overlapped);
                }
            }
            if (failed) {Disconnect(completionKey);}

        }
        else {
            SendContext* context = completionKey->inflight;
            completionKey->inflight = nullptr;
            if (context->refCount.fetch_sub(1) == 1) delete context;
            if (SendNext(completionKey)) {Disconnect(completionKey);}
        }
        ReleaseIoRef(completionKey);
    }
}

void Iocp::SendPacket(SocketInfo *socketInfo, SendContext *sendContext, DWORD messageLength) {
    sendContext->length = messageLength;

    if (socketInfo->closing) {
        if (sendContext->refCount.fetch_sub(1) == 1) delete sendContext;
        return;
    }
    socketInfo->sendQueue.push(sendContext);
    // 0 → 1로 만든 스레드만 송신 담당이 된다. 나머지는 큐에 넣고 바로 돌아간다
    if (socketInfo->pendingCount.fetch_add(1) != 0) return;
    if (socketInfo->closing) return;   // 큐에 남은 것은 ReleaseIoRef에서 정리
    if (!StartSend(socketInfo, PopSend(socketInfo))) SendNext(socketInfo);
}

// 송신 담당만 부른다. pendingCount가 메시지가 있다고 했으면 넣는 중인 노드가 연결될 때까지 잠깐 기다린다
SendContext* Iocp::PopSend(SocketInfo* s) {
    SendContext* ctx;
    while (!(ctx = s->sendQueue.pop())) std::this_thread::yield();
    return ctx;
}

// WSASend를 건다. 실패하면 메시지를 놓고 false
bool Iocp::StartSend(SocketInfo* s, SendContext* ctx) {
    s->inflight = ctx;
    if (!PostSend(s, ctx)) {
        s->inflight = nullptr;
        if (ctx->refCount.fetch_sub(1) == 1) delete ctx;
        return false;
    }
    // 거는 사이 Disconnect가 CancelIoEx를 먼저 했으면 직접 취소한다
    if (s->closing) CancelIoEx((HANDLE)s->sock, &s->sendContextIO.overlapped);
    return true;
}

// 송신 담당이 메시지 하나를 끝낸 뒤 부른다. 남은 메시지가 있으면 다음 것을 건다. 반환: 중간에 송신 실패가 있었는지
bool Iocp::SendNext(SocketInfo* s) {
    bool failed = false;
    while (s->pendingCount.fetch_sub(1) > 1) {
        if (s->closing) return failed;   // pendingCount가 0이 안 되므로 새 담당도 안 생긴다
        if (StartSend(s, PopSend(s))) return failed;
        failed = true;
    }
    return failed;
}
bool Iocp::PostSend(SocketInfo* socketInfo, SendContext* sendContext) {
    SendContextIO& io = socketInfo->sendContextIO;
    ZeroMemory(&io.overlapped, sizeof(WSAOVERLAPPED));
    io.sendBuf.buf = sendContext->buffer;
    io.sendBuf.len = sendContext->length;
    socketInfo->refCount.fetch_add(1);
    DWORD sendBytes;
    if (WSASend(socketInfo->sock, &io.sendBuf, 1, &sendBytes, 0, &io.overlapped, NULL) == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING) {
        socketInfo->refCount.fetch_sub(1);
        return false;
    }
    return true;
}

void Iocp::SetNickName(SocketInfo* socketInfo, std::string nickName) {
    socketInfo->userName = std::move(nickName);
    Service::GetInstance().InsertUserNickname(socketInfo, socketInfo->userName);
}
void Iocp::WisperMessage(SocketInfo* sender, const string& target, const string& message) {
    Service::GetInstance().ChatToUserByName(sender,target,message,*this);
}
void Iocp::BroadcastMessage(SocketInfo* sender, const string& message) {
    Service::GetInstance().ChatToAllUsers(sender,message,*this);
}
void Iocp::GetUserNicknames(SocketInfo* giveMe) {
    Service::GetInstance().GetAvailableUsersNickname(giveMe, *this);
}
/*
 * Recv Buffer 파싱
 * 맨 앞은 헤더
 * 닉네임 설정(type:1)의 경우: [이후 string] 닉네임
 * 전체 채팅의 경우(type:2): [이후 string] 채팅
 * 귓속말의 경우(type:3): [이후 string] ##wisper## 구분자를 기준으로 앞은 보낼 상대, 뒤는 보낼 메시지
 * 유저 리스트 요청의 경우(type:4): 1Byte가 끝
 */
static const char* WHISPER_DELIM = "##wisper##";

void Iocp::ProcessPacket(SocketInfo* socketInfo, char* packet, WORD size) {
    packetHeader* pHeader = (packetHeader*)packet;

    const char* payload    = packet + sizeof(packetHeader);
    size_t      payloadLen = size - sizeof(packetHeader);

    std::string body(payload, payloadLen);
    size_t nullPos = body.find('\0');
    if (nullPos != std::string::npos)
        body.resize(nullPos);

    switch (pHeader->type) {
        case 1: { // 닉네임 설정
            if (body.empty()) return;
            std::string nickname = body;
            SetNickName(socketInfo, nickname);
            break;
        }
        case 2: { // 전체 채팅
            if (body.empty()) return;
            std::string chat = body;
            BroadcastMessage(socketInfo, chat);
            break;
        }
        case 3: { // 귓속말: 받는사람##wisper##메시지
            size_t pos = body.find(WHISPER_DELIM);
            if (pos == std::string::npos) return;   // 구분자 없음 -> 잘못된 패킷

            std::string target  = body.substr(0, pos);
            std::string message = body.substr(pos + strlen(WHISPER_DELIM));

            if (target.empty() || message.empty()) return;
            WisperMessage(socketInfo, target, message);
            break;
        }
        case 4: { // 유저 리스트 요청 (헤더만 옴)
            GetUserNicknames(socketInfo);
            break;
        }
        default:
            // 알 수 없는 타입 → 무시 또는 로그/연결 종료
            break;
    }
}



bool Iocp::CreateWorker() {
    unsigned int threadId;
    unsigned int cpuCount = std::thread::hardware_concurrency();
    if (cpuCount == 0) cpuCount = 1;
    int nThreadCount = cpuCount *2;
    workerThreadHandles.reserve(nThreadCount);
    try {
        for (int i = 0; i < nThreadCount; i++) {
            workerThreadHandles.emplace_back(std::thread(&Iocp::WorkerThread, this));
        }
    }
    catch (const std::system_error& e) {
        printf_s("Worker Thread 생성 실패: %s\n", e.what());
        DestroyWorkerThread();
        return false;
    }
    printf_s("Worker Thread 시작...\n");
    return true;
}

void Iocp::DestroyWorkerThread()
{
    // 스레드마다 종료 신호 하나씩 (key = 0, overlapped = nullptr)
    for (size_t i = 0; i < workerThreadHandles.size(); ++i) {
        PostQueuedCompletionStatus(iocpHandle, 0, 0, nullptr);
    }

    for (auto& t : workerThreadHandles) {
        if (t.joinable()) t.join();
    }
    workerThreadHandles.clear();
    workerThreadRunning = false;
}

void Iocp::Disconnect(SocketInfo* s) {
    if (s->closing.exchange(true)) return;
    CancelIoEx((HANDLE)s->sock, NULL);
    Service::GetInstance().RemoveUser(s);
}
void Iocp::ReleaseIoRef(SocketInfo* s) {
    if (s->refCount.fetch_sub(1) != 1) return;
    closesocket(s->sock);
    // 이 시점엔 넣는 스레드가 없다 (RemoveUser가 끝났고 걸린 I/O도 없음)
    if (s->inflight && s->inflight->refCount.fetch_sub(1) == 1) delete s->inflight;
    while (SendContext* ctx = s->sendQueue.pop()) {
        if (ctx->refCount.fetch_sub(1) == 1) delete ctx;
    }
    delete s;
}


Iocp::~Iocp() {
    if (workerThreadRunning) DestroyWorkerThread();
    if (iocpHandle) CloseHandle(iocpHandle);
    if (listenSocket != INVALID_SOCKET) closesocket(listenSocket);
    WSACleanup();
}

Iocp::Iocp() {
    workerThreadRunning = true;
    Accepted = true;
}






