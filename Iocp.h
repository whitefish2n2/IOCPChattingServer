//
// Created by white on 26. 9. 16..
//

#ifndef IOCPSTUDY_IOCP_H
#define IOCPSTUDY_IOCP_H
#pragma comment(lib, "ws2_32.lib")
#include <map>
#include <string>
#include <thread>
#include <vector>
#include <WinSock2.h>

#include "SocketInfo.h"


struct SocketInfo;




struct packetHeader {
    DWORD messageLength;
    WORD type;
};



class Iocp {
    public:
    Iocp();
    ~Iocp();
    bool Initialize();
    void StartServer();

    void ProcessPacket(SocketInfo *socketInfo, char *packet, WORD size);

    bool CreateWorker();

    void DestroyWorkerThread();

    void Disconnect(SocketInfo *s);

    void ReleaseIoRef(SocketInfo *s);

    void WorkerThread();
    void SendPacket(SocketInfo *socketInfo, SendContext *sendContext, DWORD messageLength);

    bool PostSend(SocketInfo *socketInfo, SendContext *sendContext);

    void SetNickName(SocketInfo *socketInfo, std::string nickName);

    void WisperMessage(SocketInfo *sender, const std::string &target, const std::string &message);

    void BroadcastMessage(SocketInfo *sender, const std::string &message);

    void GetUserNicknames(SocketInfo *giveMe);

private:
    SocketInfo * m_pSocketInfo = nullptr;
    SOCKET listenSocket = INVALID_SOCKET;
    HANDLE iocpHandle = NULL;
    bool Accepted;
    bool workerThreadRunning;
    std::vector<std::thread> workerThreadHandles;

};
#endif //IOCPSTUDY_IOCP_H