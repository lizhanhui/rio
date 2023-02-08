cmake_minimum_required(VERSION 3.0)
project(rio VERSION 0.0.1 LANGUAGES CXX)

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_CXX_STANDARD 14)
set(THREADS_PREFER_PTHREAD_FLAG ON)

find_package(Threads REQUIRED)

# find_package(gflags REQUIRED)

add_executable(rio main.cpp)

target_link_libraries(rio PRIVATE uring gflags Threads::Threads)
