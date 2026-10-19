//
// Created by white on 26. 9. 29..
//

#ifndef IOCPSTUDY_SERVICE_H
#define IOCPSTUDY_SERVICE_H
#include <map>
#include <shared_mutex>
#include <string>

#include "Iocp.h"


struct SocketInfo;

class Service {
public:
    static Service& GetInstance() {
        static Service instance;
        return instance;
    }

    Service(const Service&) = delete;
    Service& operator=(const Service&) = delete;
    bool Initalize();
    void InsertUserNickname(SocketInfo* socketInfo, const std::string& nickName);
    void RemoveUser(SocketInfo *socketInfo);

    void GetAvailableUsersNickname(SocketInfo *giveMeUsers, Iocp &iocp);

    void ChatToUserByName(SocketInfo *sender, const std::string &nickName, const std::string &message, Iocp &iocp);
    void ChatToAllUsers(SocketInfo *sender, const std::string &message, Iocp &iocp);
private:
    Service() = default;
    ~Service() = default;
    std::map<std::string, SocketInfo*> socketMap;
    std::shared_mutex mutex;
};


#endif //IOCPSTUDY_SERVICE_H