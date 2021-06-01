//
// Created by Ning Wang on 2021/6/1.
//

#include <algorithm>
#include <cstdio>

#include "b_plus_tree_test_util.h"  // NOLINT
#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

TEST(BPlusTreeTests, TestInternalDistributeRight_debug) {
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
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 4, 3);
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(0);

  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  RID rid;

  for (int i = 0; i <= 8; ++i) {
    index_key.SetFromInteger(i);
    tree.Insert(index_key, rid, transaction);
  }

//  index_key.SetFromInteger(2);
//  tree.Remove(index_key);
//
//  index_key.SetFromInteger(4);
//  tree.Remove(index_key);

  tree.Print(bpm);
  tree.Draw(bpm,"/Users/ning.wang/code/bustub/build/test/pic");
//  std::string text = buffer.str();  // text will now contain "Bla\n"
//  std::string s = "Leaf Page: 1 parent: 3 next: 2\n0,1,\n\n";
//  int checkRes = s.compare(text);
//  EXPECT_EQ(checkRes, 0);
//
//  std::cout.rdbuf(oldCountStreamBuf);
}

}  // namespace bustub
