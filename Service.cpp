//
// Created by white on 26. 9. 29..
//

#include "Service.h"


#include "Iocp.h"

#define  MESSAGE_PER_THREAD 3000


void Service::InsertUserNickname(SocketInfo *socketInfo, const std::string& nickName) {
    mutex.lock();
    this->socketMap.insert(std::pair<std::string, SocketInfo*>(nickName, socketInfo));
    mutex.unlock();
}

void Service::RemoveUser(SocketInfo* socketInfo) {
    mutex.lock();
    auto v = socketMap.find(socketInfo->userName);
    if (v != socketMap.end() && v->second == socketInfo) socketMap.erase(v);
    mutex.unlock();
}

void Service::GetAvailableUsersNickname(SocketInfo* giveMeUsers, Iocp& iocp) {
    SendContext *sendContext = new SendContext();

    packetHeader* pHeader = (packetHeader*)sendContext->buffer;
    pHeader->type = 4;

    char* pBodyPtr = sendContext->buffer + sizeof(packetHeader);
    char* pCurrentPtr = pBodyPtr;

    mutex.lock_shared();

    for (auto& pair : socketMap) {
        const std::string& nickName = pair.first;
        size_t nickLen = nickName.length();

        if ((pCurrentPtr - sendContext->buffer) + nickLen + 1 > max_buffer) {
            break;
        }

        memcpy(pCurrentPtr, nickName.c_str(), nickLen);
        pCurrentPtr += nickLen;

        *pCurrentPtr = ',';
        pCurrentPtr++;
    }

    mutex.unlock_shared();
    if (pCurrentPtr > pBodyPtr) {
        pCurrentPtr--;
    }
    size_t totalPayloadSize = pCurrentPtr - pBodyPtr;
    pHeader->messageLength = sizeof(packetHeader) + (WORD)totalPayloadSize;

    if (pHeader->messageLength > max_buffer) {
        delete sendContext;
        return;
    }
    iocp.SendPacket(giveMeUsers, sendContext, pHeader->messageLength);
}


void Service::ChatToUserByName(SocketInfo* sender, const std::string &nickName, const std::string &message, Iocp& iocp) {
    if (sender->userName.empty()) return;
    auto *sendContext = new SendContext();
    auto* pHeader = (packetHeader*)sendContext->buffer;
    pHeader->type = 3;

    char* pBodyPtr = sendContext->buffer+sizeof(packetHeader);

    size_t nameLen = sender->userName.length();
    memcpy(pBodyPtr, sender->userName.c_str(), nameLen);
    pBodyPtr += nameLen;
    memcpy(pBodyPtr, " | ", 3);

    pBodyPtr += 3;
    const size_t slashLen = 3;

    size_t msgLen = message.length();
    pHeader->messageLength = sizeof(packetHeader) + (WORD)nameLen + (WORD)slashLen + (WORD)message.length();
    if (pHeader->messageLength > max_buffer) {
        delete sendContext;
        return;
    }
    memcpy(pBodyPtr, message.c_str(), msgLen);
    mutex.lock_shared();
    auto it = this->socketMap.find(nickName);
    if (it == socketMap.end()) {mutex.unlock_shared(); return;}
    auto* socketInfo = it->second;
    iocp.SendPacket(socketInfo, sendContext, pHeader->messageLength);
    mutex.unlock_shared();
}

bool Service::Initalize() {
    this->socketMap.clear();
    return true;
}



void Service::ChatToAllUsers(SocketInfo* sender, const std::string &message, Iocp& iocp) {

    if (sender->userName.empty()) return;

    auto *sendContext = new SendContext();
    auto* pHeader = (packetHeader*)sendContext->buffer;
    pHeader->type = 2;
    char* pBodyPtr = sendContext->buffer+sizeof(packetHeader);

    size_t nameLen = sender->userName.length();
    memcpy(pBodyPtr, sender->userName.c_str(), nameLen);
    pBodyPtr += nameLen;
    memcpy(pBodyPtr, " | ", 3);

    pBodyPtr += 3;
    const size_t slashLen = 3;

    DWORD msgLen = message.length();
    pHeader->messageLength = sizeof(packetHeader) + (WORD)nameLen + (WORD)slashLen + (WORD)message.length();
    if (pHeader->messageLength > max_buffer) {
        delete sendContext;
        return;
    }
    memcpy(pBodyPtr, message.c_str(), msgLen);
    mutex.lock_shared();
    sendContext->refCount.store(this->socketMap.size());
    if (sendContext->refCount>0) {
        auto current_start = socketMap.begin();
        auto total_end = socketMap.end();
        int threadCount = (socketMap.size()/MESSAGE_PER_THREAD) + ((socketMap.size()%MESSAGE_PER_THREAD == 0) ? 0:1);
        std::vector<std::jthread> threads;
        threads.reserve(threadCount);
        for (int i = 0; i < threadCount; i++) {
            auto remaining_elements = std::distance(current_start, total_end);
            auto current_chunk = (std::min)(static_cast<long long>(MESSAGE_PER_THREAD), remaining_elements);
            auto current_end = std::next(current_start, current_chunk);
            threads.emplace_back(&Service::BroadcastByRange,this, current_start,current_end, sendContext,pHeader->messageLength, &iocp);
            current_start = current_end;
        }
    }
    else {
        mutex.unlock_shared();
        delete sendContext;
        return;
    }

    mutex.unlock_shared();
}

void Service::BroadcastByRange(std::map<std::string, SocketInfo*>::iterator from, std::map<std::string, SocketInfo*>::iterator to, SendContext *context, DWORD length, Iocp* iocp) {
    for (auto it = from; it != to; it++) {
        iocp->SendPacket(it->second, context, length);
    }
}


