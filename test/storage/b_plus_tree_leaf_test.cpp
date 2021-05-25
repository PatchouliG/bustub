/**
 * b_plus_tree_insert_test.cpp
 */

#include <algorithm>
#include <cstdio>

#include "b_plus_tree_test_util.h"  // NOLINT
#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

TEST(BPlusTreeTests, test_init) {
  void *page = malloc(1024);
  auto *leaf_node = static_cast<BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>> *>(page);
  leaf_node->Init(1, 2, 3);
  EXPECT_EQ(leaf_node->GetParentPageId(), 2);
  EXPECT_EQ(leaf_node->GetPageId(), 1);
  EXPECT_EQ(leaf_node->GetMaxSize(), 3);
  EXPECT_EQ(leaf_node->GetSize(), 0);
}  // namespace bustub
TEST(BPlusTreeTests, test_insert) {
  void *page = malloc(1024);
  auto *leaf_node = (BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>> *)(page);
  leaf_node->Init(1, 2, 100);
  auto key = GenericKey<64>();
  Schema *key_schema = ParseCreateStatement("a bigint");
  auto comp = GenericComparator<64>(key_schema);
  key.SetFromInteger(2);
  leaf_node->Insert(key, RID(), comp);

  key.SetFromInteger(1);
  leaf_node->Insert(key, RID(), comp);
  //
  key.SetFromInteger(3);
  auto res = leaf_node->Insert(key, RID(), comp);
  EXPECT_EQ(res, 3);
  //  check
  key.SetFromInteger(1);
  auto position = leaf_node->KeyIndex(key, comp);
  EXPECT_EQ(position, 0);
  //  check
  key.SetFromInteger(3);
  position = leaf_node->KeyIndex(key, comp);
  EXPECT_EQ(position, 2);
  //  not found
  key.SetFromInteger(999);
  position = leaf_node->KeyIndex(key, comp);
  EXPECT_EQ(position, -1);

  //  check key at
  auto key_res = leaf_node->KeyAt(1);
  EXPECT_EQ(key_res.ToString(), 2);

  //  check look at
  RID rid = RID();
  key.SetFromInteger(999);
  auto look_at_res = leaf_node->Lookup(key, &rid, comp);
  EXPECT_FALSE(look_at_res);
  key.SetFromInteger(1);
  look_at_res = leaf_node->Lookup(key, &rid, comp);
  EXPECT_TRUE(look_at_res);
}

TEST(BPlusTreeTests, test_KeyIndex) {
  void *page = malloc(1024);
  auto *leaf_node = (BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>> *)(page);
  leaf_node->Init(1, 2, 3);
  auto key = GenericKey<64>();
  key.SetFromInteger(234);
  Schema *key_schema = ParseCreateStatement("a bigint");
  auto comp = GenericComparator<64>(key_schema);

  leaf_node->Insert(key, RID(), comp);
  leaf_node->KeyIndex(key, comp);
}
}  // namespace bustub
