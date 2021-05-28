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

Schema *key_schema = ParseCreateStatement("a bigint");
GenericComparator<64> comparator(key_schema);

DiskManager *disk_manager = new DiskManager("test.db");
BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);

TEST(BPlusTreeTests, test_init) {
  auto nodeWrap = NodePageWrap<GenericKey<64>, RID, GenericComparator<64>>(bpm, IndexPageType::INTERNAL_PAGE, 3);
  auto node = nodeWrap.toInternalPage();
  EXPECT_EQ(node->GetPageId(), 0);
  EXPECT_EQ(node->GetMaxSize(), 3);
  EXPECT_EQ(node->GetSize(), 0);
}

TEST(BPlusTreeTests, test_populateNewRoot) {
  auto nodeWrap = NodePageWrap<GenericKey<64>, RID, GenericComparator<64>>(bpm, IndexPageType::INTERNAL_PAGE, 10);
  auto internal_node = nodeWrap.toMutableInternalPage();

  auto key = GenericKey<64>();
  key.SetFromInteger(1);

  internal_node->PopulateNewRoot(1, key, 2);

  EXPECT_EQ(internal_node->GetParentPageId(), -1);
  EXPECT_EQ(internal_node->GetMaxSize(), 10);
  EXPECT_EQ(internal_node->GetSize(), 2);
}

}  // namespace bustub