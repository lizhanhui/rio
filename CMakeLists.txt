cmake_minimum_required(VERSION 3.0)
project(rio VERSION 0.0.1 LANGUAGES CXX)

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_CXX_STANDARD 14)

# find_package(gflags REQUIRED)

add_executable(rio main.cpp)

target_link_libraries(rio uring gflags)
