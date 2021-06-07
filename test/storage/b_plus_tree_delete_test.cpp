/**
 * b_plus_tree_delete_test.cpp
 */

#include <algorithm>
#include <cstdio>

#include "b_plus_tree_test_util.h"  // NOLINT
#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

TEST(BPlusTreeTests, DISABLED_DeleteTest1) {
  // create KeyComparator and index schema
  std::string createStmt = "a bigint";
  Schema *key_schema = ParseCreateStatement(createStmt);
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

  std::vector<int64_t> remove_keys = {1, 5};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  start_key = 2;
  current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 3);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, DISABLED_DeleteTest2) {
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

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  start_key = 2;
  current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, DeleteTestEmpty) {
  std::string createStmt = "a bigint";
  Schema *key_schema = ParseCreateStatement(createStmt);
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(0);

  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  index_key.SetFromInteger(0);
  tree.Remove(index_key, transaction);

  delete disk_manager;
  delete key_schema;
  delete bpm;
  delete transaction;
}
TEST(BPlusTreeTests, DeleteTestNotFound) {
  std::string createStmt = "a bigint";
  Schema *key_schema = ParseCreateStatement(createStmt);
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(0);

  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  RID rid;

  index_key.SetFromInteger(1);
  tree.Insert(index_key, rid, transaction);
  index_key.SetFromInteger(2);
  tree.Remove(index_key, transaction);

  tree.Print(bpm);

  delete disk_manager;
  delete key_schema;
  delete bpm;
  delete transaction;
}
TEST(BPlusTreeTests, DeleteTestRootIsLeaf) {
  std::string createStmt = "a bigint";
  Schema *key_schema = ParseCreateStatement(createStmt);
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(0);

  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  RID rid;

  index_key.SetFromInteger(1);
  tree.Insert(index_key, rid, transaction);
  index_key.SetFromInteger(2);
  tree.Insert(index_key, rid, transaction);
  index_key.SetFromInteger(2);
  tree.Remove(index_key, transaction);
  index_key.SetFromInteger(1);
  tree.Remove(index_key, transaction);

  tree.Print(bpm);

  delete disk_manager;
  delete key_schema;
  delete bpm;
  delete transaction;
}

TEST(BPlusTreeTests, TestDeleteAll) {
  std::stringstream buffer;
  std::streambuf *oldCoutStreamBuf = std::cout.rdbuf();

  std::cout.rdbuf(buffer.rdbuf());

  std::string createStmt = "a bigint";
  Schema *key_schema = ParseCreateStatement(createStmt);
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(0);

  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  RID rid;

  index_key.SetFromInteger(1);
  tree.Insert(index_key, rid, transaction);
  index_key.SetFromInteger(2);
  tree.Insert(index_key, rid, transaction);
  index_key.SetFromInteger(2);
  tree.Remove(index_key, transaction);
  index_key.SetFromInteger(1);
  tree.Remove(index_key, transaction);

  tree.Print(bpm);

  std::string text = buffer.str();  // text will now contain "Bla\n"
  std::string s = "Leaf Page: 1 parent: -1 next: -1\n\n\n";
  int checkRes = s.compare(text);
  EXPECT_EQ(checkRes, 0);
  std::cout.rdbuf(oldCoutStreamBuf);

  delete disk_manager;
  delete key_schema;
  delete bpm;
  delete transaction;
}
TEST(BPlusTreeTests, TestDisTributeFromRight) {
  std::stringstream buffer;
  std::streambuf *oldCoutStreamBuf = std::cout.rdbuf();

  std::cout.rdbuf(buffer.rdbuf());

  std::string createStmt = "a bigint";
  Schema *key_schema = ParseCreateStatement(createStmt);
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 4, 10);
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(0);

  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  RID rid;

  for (int i = 1; i <= 6; ++i) {
    index_key.SetFromInteger(i);
    tree.Insert(index_key, rid, transaction);
  }

  index_key.SetFromInteger(1);
  tree.Remove(index_key);

  index_key.SetFromInteger(3);
  tree.Remove(index_key);

  tree.Print(bpm);
  std::string text = buffer.str();  // text will now contain "Bla\n"
  std::string s =
      "Internal Page: 3 parent: -1\n0: 1,5: 2,\n\nLeaf Page: 1 parent: 3 next: 2\n2,4,\n\nLeaf Page: 2 parent: 3 next: "
      "-1\n5,6,\n\n";
  int checkRes = s.compare(text);
  EXPECT_EQ(checkRes, 0);
  std::cout.rdbuf(oldCoutStreamBuf);

  delete disk_manager;
  delete key_schema;
  delete bpm;
  delete transaction;
}

TEST(BPlusTreeTests, TestDisTributeFromLeft) {
  std::stringstream buffer;
  std::streambuf *oldCoutStreamBuf = std::cout.rdbuf();

  std::cout.rdbuf(buffer.rdbuf());

  std::string createStmt = "a bigint";
  Schema *key_schema = ParseCreateStatement(createStmt);
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 4, 10);
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(0);

  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  RID rid;

  for (int i = 0; i <= 4; ++i) {
    index_key.SetFromInteger(i);
    tree.Insert(index_key, rid, transaction);
  }

  index_key.SetFromInteger(-1);
  tree.Insert(index_key, rid, transaction);

  index_key.SetFromInteger(2);
  tree.Remove(index_key);

  index_key.SetFromInteger(3);
  tree.Remove(index_key);

  tree.Print(bpm);
  std::string text = buffer.str();  // text will now contain "Bla\n"
  std::string s =
      "Internal Page: 3 parent: -1\n0: 1,1: 2,\n\nLeaf Page: 1 parent: 3 next: 2\n-1,0,\n\nLeaf Page: 2 parent: 3 "
      "next: -1\n1,4,\n\n";
  int checkRes = s.compare(text);
  EXPECT_EQ(checkRes, 0);

  std::cout.rdbuf(oldCoutStreamBuf);

  delete disk_manager;
  delete key_schema;
  delete bpm;
  delete transaction;
}
TEST(BPlusTreeTests, TestLeafMergeRight) {
  std::stringstream buffer;
  std::streambuf *oldCountStreamBuf = std::cout.rdbuf();

  std::cout.rdbuf(buffer.rdbuf());

  std::string createStmt = "a bigint";
  Schema *key_schema = ParseCreateStatement(createStmt);
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 4, 10);
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(0);

  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  RID rid;

  for (int i = 0; i <= 4; ++i) {
    index_key.SetFromInteger(i);
    tree.Insert(index_key, rid, transaction);
  }

  index_key.SetFromInteger(2);
  tree.Remove(index_key);

  index_key.SetFromInteger(4);
  tree.Remove(index_key);

  tree.Print(bpm);
  tree.Draw(bpm,"pic");
  std::string text = buffer.str();  // text will now contain "Bla\n"
  std::string s = "Leaf Page: 1 parent: 3 next: -1\n0,1,3,\n\n";
  int checkRes = s.compare(text);
  EXPECT_EQ(checkRes, 0);

  std::cout.rdbuf(oldCountStreamBuf);

  delete disk_manager;
  delete key_schema;
  delete bpm;
  delete transaction;
}

TEST(BPlusTreeTests, TestInternalDistributeFromRight) {
//  std::stringstream buffer;
//  std::streambuf *oldCountStreamBuf = std::cout.rdbuf();
//
//  std::cout.rdbuf(buffer.rdbuf());

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

  for (int i = 0; i <= 15; ++i) {
    index_key.SetFromInteger(i);
    tree.Insert(index_key, rid, transaction);
  }

  index_key.SetFromInteger(6);
  tree.Remove(index_key);

  index_key.SetFromInteger(7);
  tree.Remove(index_key);

  tree.Print(bpm);
  tree.Draw(bpm,"pic");
//  std::string text = buffer.str();  // text will now contain "Bla\n"
//  std::string s = "Leaf Page: 1 parent: 3 next: 2\n0,1,\n\n";
//  int checkRes = s.compare(text);
//  EXPECT_EQ(checkRes, 0);

//  std::cout.rdbuf(oldCountStreamBuf);

  delete disk_manager;
  delete key_schema;
  delete bpm;
  delete transaction;

}
TEST(BPlusTreeTests, TestInternalDistributeLeft) {}

TEST(BPlusTreeTests, TestInternalMergeRight) {}
TEST(BPlusTreeTests, TestInternalMergeLeft) {}
TEST(BPlusTreeTests, TestDeleteMutipleTime) {}
// namespace bustub
}  // namespace bustub
