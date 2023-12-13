add_executable(UnitTests test_p0.cpp)
include_directories(${GTEST_INCLUDE_DIRS})
target_link_libraries(UnitTests gtest gtest_main p0_trie -pthread)
