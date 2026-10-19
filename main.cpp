#pragma comment(lib, "ws2_32.lib")
#include <iostream>
#include <string>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <thread>
#include <vector>
#include <chrono>

#include "Iocp.h"
#include "Service.h"

int main() {
    Iocp iocp;
    Service::GetInstance().Initalize();
    if (iocp.Initialize()) {
        iocp.StartServer();
    }
    return 0;
}
