# 设置 Google Test 的路径
cmake_minimum_required(VERSION 3.10)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)   # For clang-tidy.
set(BUILD_SHARED_LIBS OFF)              # We expect external libraries to be linked statically.
set(CMAKE_CXX_STANDARD 17)              # Compile as C++17.
set(CMAKE_CXX_STANDARD_REQUIRED ON)     # Require C++17 support.

set(GTEST_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/external/googletest")
find_package(GTest REQUIRED)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
include_directories(${GTEST_INCLUDE_DIRS})  # 添加这一行

add_library(p0_trie p0_trie.cpp p0_trie.h)
target_include_directories(p0_trie PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_subdirectory(test)
