// test_p0.cpp
#include "primer/p0_trie.h"
#include "gtest/gtest.h"

TEST(P0_TRIE, GET_VALUE) {
  bustub::Trie *trie = new bustub::Trie();
  bool success = false;
  int result = trie->GetValue<int>("test", &success);
  EXPECT_EQ(result, 0);
}

TEST(P0_TRIE, INSERT) {
  bustub::Trie *trie = new bustub::Trie();
  bool success = trie->Insert<int>("test", 10);
  EXPECT_EQ(success, true);
  success = trie->Insert<int>("tes", 11);
  EXPECT_EQ(success, true);
  success = trie->Insert<int>("tes", 12);
  EXPECT_EQ(success, false);
  success = trie->Insert<int>("testremove", 13);
  EXPECT_EQ(success, true);
  success = trie->Remove("testremove");
  EXPECT_EQ(success, true);

  int result = trie->GetValue<int>("test", &success);
  EXPECT_EQ(success, true);
  EXPECT_EQ(result, 10);

  result = trie->GetValue<int>("tes", &success);
  EXPECT_EQ(success, true);
  EXPECT_EQ(result, 11);

  result = trie->GetValue<int>("testr", &success);
  EXPECT_EQ(success, false);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
