/**
 * b_plus_tree_test.cpp
 */

#include <chrono>  // NOLINT
#include <cstdio>
#include <functional>
#include <thread>                   // NOLINT
#include "b_plus_tree_test_util.h"  // NOLINT

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {
TEST(BPlusTreeTests, test_page_wrap_pin_count) {
  // create KeyComparator and index schema
  std::string createStmt = "a bigint";
  Schema *key_schema = ParseCreateStatement(createStmt);
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(100, disk_manager);
  Page *a_page, *b_page;
  {
    NodePageWrap a = NodePageWrap<GenericKey<4>, RID, GenericComparator<4>>(bpm, IndexPageType::LEAF_PAGE, 10);
    EXPECT_EQ(a.getPage()->GetPinCount(), 1);

    NodePageWrap b = NodePageWrap<GenericKey<4>, RID, GenericComparator<4>>(bpm, IndexPageType::LEAF_PAGE, 10);
    EXPECT_EQ(b.getPage()->GetPinCount(), 1);
    b_page = b.getPage();

    {
      NodePageWrap c = NodePageWrap<GenericKey<4>, RID, GenericComparator<4>>(a.getPageId(), bpm);
      EXPECT_EQ(c.getPage()->GetPinCount(), 2);
      a_page = c.getPage();
    }
    EXPECT_EQ(a_page->GetPinCount(), 1);

    NodePageWrap d = a;
    EXPECT_EQ(a_page->GetPinCount(), 2);
  }
  EXPECT_EQ(a_page->GetPinCount(), 0);
  EXPECT_EQ(b_page->GetPinCount(), 0);

  delete key_schema;
  delete disk_manager;
  delete bpm;

}  // namespace bustu
}  // namespace bustub
