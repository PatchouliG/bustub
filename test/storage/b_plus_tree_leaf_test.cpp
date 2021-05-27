/**
 * b_plus_tree_insert_test.cpp
 */

#include <algorithm>
#include <cstdio>
#include <stack>

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
//  void *page = malloc(1024);
//  auto *leaf_node = static_cast<BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>> *>(page);
//  leaf_node->Init(1, 2, 3);
  auto nodeWrap = NodePageWrap<GenericKey<64>, RID, GenericComparator<64>>(bpm, IndexPageType::LEAF_PAGE, 10);
  auto *leaf_node = nodeWrap.toLeafPage();
  EXPECT_EQ(leaf_node->GetPageId(), 0);
  EXPECT_EQ(leaf_node->GetMaxSize(), 10);
  EXPECT_EQ(leaf_node->GetSize(), 0);
}  // namespace bustub
TEST(BPlusTreeTests, test_insert) {
  auto nodeWrap = NodePageWrap<GenericKey<64>, RID, GenericComparator<64>>(bpm, IndexPageType::LEAF_PAGE, 10);
  auto *leaf_node = nodeWrap.toLeafPage();
//  auto *leaf_node = nodeWrap.toLeafPage();
//  void *page = malloc(1024);
//  auto *leaf_node = (BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>> *)(page);
  leaf_node->Init(1, 2, 100);
  auto key = GenericKey<64>();
  //  Schema *key_schema = ParseCreateStatement("a bigint");
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
  auto nodeWrap = NodePageWrap<GenericKey<64>, RID, GenericComparator<64>>(bpm, IndexPageType::LEAF_PAGE, 10);
  auto *leaf_node = nodeWrap.toLeafPage();
  auto key = GenericKey<64>();
  key.SetFromInteger(234);

  leaf_node->Insert(key, RID(), comparator);
  leaf_node->KeyIndex(key, comparator);

  auto keys = leaf_node->Keys();
  EXPECT_EQ(keys.size(), 1);
  EXPECT_EQ(keys[0], 234);
}
}  // namespace bustub
