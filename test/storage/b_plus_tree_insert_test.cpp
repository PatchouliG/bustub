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

TEST(BPlusTreeTests, InsertTest1) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, InsertTest2) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {5, 4, 3, 2, 1};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  start_key = 3;
  current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, test_insert_in_order) {
  std::stringstream buffer;
  std::streambuf *oldCountStreamBuf = std::cout.rdbuf();

  std::cout.rdbuf(buffer.rdbuf());

  std::string createStmt = "a bigint";
  Schema *key_schema = ParseCreateStatement(createStmt);
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(0);

  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  RID rid;

  for (int i = 0; i <= 9; ++i) {
    index_key.SetFromInteger(i);
    tree.Insert(index_key, rid, transaction);
  }

  //  index_key.SetFromInteger(2);
  //  tree.Remove(index_key);

  //  index_key.SetFromInteger(4);
  //  tree.Remove(index_key);

  tree.Print(bpm);
  tree.Draw(bpm, "pic");
  std::string text = buffer.str();  // text will now contain "Bla\n"
  std::string s =
      "Internal Page: 8 parent: -1\n0: 3,2: 7,4: 11,6: 14,\n\nInternal Page: 3 parent: 8\n0: 1,1: 2,\n\nLeaf Page: 1 "
      "parent: 3 next: 2\n0,\n\nLeaf Page: 2 parent: 3 next: 4\n1,\n\nInternal Page: 7 parent: 8\n2: 4,3: 5,\n\nLeaf "
      "Page: 4 parent: 7 next: 5\n2,\n\nLeaf Page: 5 parent: 7 next: 6\n3,\n\nInternal Page: 11 parent: 8\n4: 6,5: "
      "9,\n\nLeaf Page: 6 parent: 11 next: 9\n4,\n\nLeaf Page: 9 parent: 11 next: 10\n5,\n\nInternal Page: 14 "
      "parent: 8\n6: 10,7: 12,8: 13,\n\nLeaf Page: 10 parent: 14 next: 12\n6,\n\nLeaf Page: 12 parent: 14 next: "
      "13\n7,\n\nLeaf Page: 13 parent: 14 next: -1\n8,9,\n\n";
  int checkRes = s.compare(text);
  EXPECT_EQ(checkRes, 0);
  //
  std::cout.rdbuf(oldCountStreamBuf);

  delete disk_manager;
  delete key_schema;
  delete bpm;
  delete transaction;
}

TEST(BPlusTreeTests, test_insert_out_of_order) {
  std::stringstream buffer;
  std::streambuf *oldCountStreamBuf = std::cout.rdbuf();

  std::cout.rdbuf(buffer.rdbuf());

  std::string createStmt = "a bigint";
  Schema *key_schema = ParseCreateStatement(createStmt);
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 3, 3);
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(0);

  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  RID rid;
  std::vector v{1, 2, -1, 0, 9, 3, 6, 11, 8, 345, -34, 83, 50, 34};

  for (size_t i = 0; i < v.size(); ++i) {
    index_key.SetFromInteger(v[i]);
    tree.Insert(index_key, rid, transaction);
  }

  tree.Print(bpm);
//  tree.Draw(bpm, "pic");
  std::string text = buffer.str();  // text will now contain "Bla\n"
  std::string s =
      "Internal Page: 8 parent: -1\n0: 3,3: 7,\n\nInternal Page: 3 parent: 8\n0: 1,1: 2,\n\nLeaf Page: 1 parent: 3 "
      "next: 2\n-34,-1,0,\n\nLeaf Page: 2 parent: 3 next: 4\n1,2,\n\nInternal Page: 7 parent: 8\n3: 4,9: 5,34: 9,83: "
      "6,\n\nLeaf Page: 4 parent: 7 next: 5\n3,6,8,\n\nLeaf Page: 5 parent: 7 next: 9\n9,11,\n\nLeaf Page: 9 parent: 7 "
      "next: -1\n34,50,\n\nLeaf Page: 6 parent: 7 next: -1\n83,345,\n\n";
  int checkRes = s.compare(text);
  EXPECT_EQ(checkRes, 0);
  std::cout.rdbuf(oldCountStreamBuf);

  delete disk_manager;
  delete key_schema;
  delete bpm;
  delete transaction;
}

}  // namespace bustub
