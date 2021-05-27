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
// todo use nodeWrap
BPlusTreeInternalPage<GenericKey<64>, page_id_t , GenericComparator<64>> *getNode() {
  void *page = malloc(1024);
  auto *node = (BPlusTreeInternalPage<GenericKey<64>, page_id_t , GenericComparator<64>> *)(page);
  node->Init(1, 2, 3);
  return node;
}

TEST(BPlusTreeTests, test_init) {
  auto *node = getNode();
  node->Init(1, 2, 3);
  EXPECT_EQ(node->GetParentPageId(), 2);
  EXPECT_EQ(node->GetPageId(), 1);
  EXPECT_EQ(node->GetMaxSize(), 3);
  EXPECT_EQ(node->GetSize(), 0);
}
// namespace bustub

// todo
TEST(BPlusTreeTests, test_todo) {
  void *page = malloc(1024);
  auto *internal_node = (BPlusTreeInternalPage<GenericKey<64>, page_id_t , GenericComparator<64>> *)(page);
  internal_node->Init(1, 2, 3);

  auto key = GenericKey<64>();
  key.SetFromInteger(1);

  internal_node->PopulateNewRoot(1, key, 2);

  EXPECT_EQ(internal_node->GetParentPageId(), 2);
  EXPECT_EQ(internal_node->GetPageId(), 1);
  EXPECT_EQ(internal_node->GetMaxSize(), 3);
  EXPECT_EQ(internal_node->GetSize(), 1);
}  // namespace bustub

}  // namespace bustub