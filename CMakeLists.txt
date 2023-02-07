cmake_minimum_required(VERSION 3.0)
project(rio VERSION 0.0.1 LANGUAGES CXX)

add_executable(rio main.cpp)
target_link_libraries(rio uring)
