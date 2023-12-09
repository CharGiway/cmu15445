// test_p0.cpp
#include "../p0_trie.h"
#include "gtest/gtest.h"
using namespace bustub;

TEST(P0_TRIE, GET_VALUE) {
  Trie *trie = new Trie();
  bool success = false;
  int result = trie->GetValue<int>("test", &success);
  EXPECT_EQ(result, 0);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
