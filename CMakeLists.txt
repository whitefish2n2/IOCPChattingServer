cmake_minimum_required(VERSION 4.0)
project(IOCPStudy)

set(CMAKE_CXX_STANDARD 20)

add_executable(IOCPStudy main.cpp
        Iocp.h
        Iocp.cpp
        Service.cpp
        Service.h
        SocketInfo.h)

add_executable(ChatClient client/ChatClient.cpp)
target_link_libraries(ChatClient ws2_32)
if (MSVC)
    target_compile_options(ChatClient PRIVATE /utf-8)
endif ()
