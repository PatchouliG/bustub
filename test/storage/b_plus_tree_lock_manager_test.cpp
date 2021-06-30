//
// Created by wn on 2021/6/29.
//

#include <algorithm>
#include <cstdio>

#include "b_plus_tree_test_util.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

#define PageType NodePageWrap<GenericKey<8>, RID, GenericComparator<8>>

TEST(BPlusTreeTests, lockManager_test_read_mod) {
  Schema *key_schema = ParseCreateStatement("a bigint");

  GenericComparator<8> comparator(key_schema);
  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  typedef BPlusTree<GenericKey<8>, RID, GenericComparator<8>> BTree;
  BTree tree("foo_pk", bpm, comparator, 3, 2);

  {
    auto rootNode = PageType(bpm, IndexPageType::INTERNAL_PAGE, 3);
    auto node2 = PageType(bpm, IndexPageType::INTERNAL_PAGE, 3);
    auto node3 = PageType(bpm, IndexPageType::LEAF_PAGE, 3);
    {
      BTree::BTreeLockManager bTreeLockManager(&tree, BTree::Mode::read);

      //  add 3 child page
      //      check all page is rLock
      //      check root mutex lock
      bTreeLockManager.addChild(rootNode);
      EXPECT_EQ(rootNode.getPage()->getLockStatus(), Page::LockStatus::rlock);
      bTreeLockManager.addChild(node2);
      EXPECT_EQ(node2.getPage()->getLockStatus(), Page::LockStatus::rlock);
      EXPECT_EQ(rootNode.getPage()->getLockStatus(), Page::LockStatus::unlock);
      bTreeLockManager.addChild(node3);
      EXPECT_EQ(node3.getPage()->getLockStatus(), Page::LockStatus::rlock);
      EXPECT_EQ(node2.getPage()->getLockStatus(), Page::LockStatus::unlock);
      bTreeLockManager.popChild();
      EXPECT_EQ(node3.getPage()->getLockStatus(), Page::LockStatus::unlock);
    }
    EXPECT_EQ(rootNode.getPage()->getLockStatus(), Page::LockStatus::unlock);
    EXPECT_EQ(node2.getPage()->getLockStatus(), Page::LockStatus::unlock);
    EXPECT_EQ(node3.getPage()->getLockStatus(), Page::LockStatus::unlock);
    //    run out of scope check all page is unlock check root mutex unlock
  }
  delete key_schema;
  delete disk_manager;
  delete bpm;
}
TEST(BPlusTreeTests, lockManager_test_insert_mod_with_full_node) {
  Schema *key_schema = ParseCreateStatement("a bigint");

  GenericComparator<8> comparator(key_schema);
  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  typedef BPlusTree<GenericKey<8>, RID, GenericComparator<8>> BTree;
  BTree tree("foo_pk", bpm, comparator, 3, 2);

  {
    auto rootNode = PageType(bpm, IndexPageType::INTERNAL_PAGE, 3);
    rootNode.toMutableBPlusTreePage()->SetSize(3);
    auto node2 = PageType(bpm, IndexPageType::INTERNAL_PAGE, 3);
    node2.toMutableBPlusTreePage()->SetSize(3);
    auto node3 = PageType(bpm, IndexPageType::LEAF_PAGE, 3);
    node3.toMutableBPlusTreePage()->SetSize(3);
    {
      BTree::BTreeLockManager bTreeLockManager(&tree, BTree::Mode::insert);

      //  add 3 child page
      //      check all page is rLock
      //      check root mutex lock
      bTreeLockManager.addChild(rootNode);
      bTreeLockManager.addChild(node2);
      bTreeLockManager.addChild(node3);

      EXPECT_EQ(rootNode.getPage()->getLockStatus(), Page::LockStatus::wlock);
      EXPECT_EQ(node2.getPage()->getLockStatus(), Page::LockStatus::wlock);
      EXPECT_EQ(node3.getPage()->getLockStatus(), Page::LockStatus::wlock);

      bTreeLockManager.popChild();
      EXPECT_EQ(node3.getPage()->getLockStatus(), Page::LockStatus::unlock);
    }
    EXPECT_EQ(rootNode.getPage()->getLockStatus(), Page::LockStatus::unlock);
    EXPECT_EQ(node2.getPage()->getLockStatus(), Page::LockStatus::unlock);
    EXPECT_EQ(node3.getPage()->getLockStatus(), Page::LockStatus::unlock);
  }
  //      run out of scope
  //      check all page is unlock
  //      check root mutex unlock
  delete key_schema;
  delete disk_manager;
  delete bpm;
}
TEST(BPlusTreeTests, lockManager_test_insert_mod_with_un_full_node) {
  Schema *key_schema = ParseCreateStatement("a bigint");

  GenericComparator<8> comparator(key_schema);
  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  typedef BPlusTree<GenericKey<8>, RID, GenericComparator<8>> BTree;
  BTree tree("foo_pk", bpm, comparator, 3, 2);

  {
    auto rootNode = PageType(bpm, IndexPageType::INTERNAL_PAGE, 3);
    rootNode.toMutableBPlusTreePage()->SetSize(2);
    auto node2 = PageType(bpm, IndexPageType::INTERNAL_PAGE, 3);
    rootNode.toMutableBPlusTreePage()->SetSize(2);
    auto node3 = PageType(bpm, IndexPageType::LEAF_PAGE, 3);
    rootNode.toMutableBPlusTreePage()->SetSize(2);
    {
      BTree::BTreeLockManager bTreeLockManager(&tree, BTree::Mode::insert);

      //  add 3 child page
      //      check all page is rLock
      //      check root mutex lock
      bTreeLockManager.addChild(rootNode);
      EXPECT_EQ(rootNode.getPage()->getLockStatus(), Page::LockStatus::wlock);

      bTreeLockManager.addChild(node2);
      EXPECT_EQ(rootNode.getPage()->getLockStatus(), Page::LockStatus::unlock);
      EXPECT_EQ(node2.getPage()->getLockStatus(), Page::LockStatus::wlock);

      bTreeLockManager.addChild(node3);
      EXPECT_EQ(node2.getPage()->getLockStatus(), Page::LockStatus::unlock);
      EXPECT_EQ(node3.getPage()->getLockStatus(), Page::LockStatus::wlock);

      bTreeLockManager.popChild();
      EXPECT_EQ(node3.getPage()->getLockStatus(), Page::LockStatus::unlock);
    }
    EXPECT_EQ(rootNode.getPage()->getLockStatus(), Page::LockStatus::unlock);
    EXPECT_EQ(node2.getPage()->getLockStatus(), Page::LockStatus::unlock);
    EXPECT_EQ(node3.getPage()->getLockStatus(), Page::LockStatus::unlock);
  }
  //      run out of scope
  //      check all page is unlock
  //      check root mutex unlock
  delete key_schema;
  delete disk_manager;
  delete bpm;
}
TEST(BPlusTreeTests, lockManager_test_remove_mode_with_full_node) {
  Schema *key_schema = ParseCreateStatement("a bigint");

  GenericComparator<8> comparator(key_schema);
  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  typedef BPlusTree<GenericKey<8>, RID, GenericComparator<8>> BTree;
  BTree tree("foo_pk", bpm, comparator, 3, 2);

  {
    auto rootNode = PageType(bpm, IndexPageType::INTERNAL_PAGE, 3);
    rootNode.toMutableBPlusTreePage()->SetSize(3);
    auto node2 = PageType(bpm, IndexPageType::INTERNAL_PAGE, 3);
    node2.toMutableBPlusTreePage()->SetSize(3);
    auto node3 = PageType(bpm, IndexPageType::LEAF_PAGE, 3);
    node3.toMutableBPlusTreePage()->SetSize(3);
    {
      BTree::BTreeLockManager bTreeLockManager(&tree, BTree::Mode::remove);

      //  add 3 child page
      //      check all page is rLock
      //      check root mutex lock
      bTreeLockManager.addChild(rootNode);
      EXPECT_EQ(rootNode.getPage()->getLockStatus(), Page::LockStatus::wlock);

      bTreeLockManager.addChild(node2);
      EXPECT_EQ(rootNode.getPage()->getLockStatus(), Page::LockStatus::unlock);
      EXPECT_EQ(node2.getPage()->getLockStatus(), Page::LockStatus::wlock);

      bTreeLockManager.addChild(node3);
      EXPECT_EQ(node2.getPage()->getLockStatus(), Page::LockStatus::unlock);
      EXPECT_EQ(node3.getPage()->getLockStatus(), Page::LockStatus::wlock);

      bTreeLockManager.popChild();
      EXPECT_EQ(node3.getPage()->getLockStatus(), Page::LockStatus::unlock);
    }
    EXPECT_EQ(rootNode.getPage()->getLockStatus(), Page::LockStatus::unlock);
    EXPECT_EQ(node2.getPage()->getLockStatus(), Page::LockStatus::unlock);
    EXPECT_EQ(node3.getPage()->getLockStatus(), Page::LockStatus::unlock);
  }
  //      run out of scope
  //      check all page is unlock
  //      check root mutex unlock
  delete key_schema;
  delete disk_manager;
  delete bpm;
}
TEST(BPlusTreeTests, lockManager_test_remove_mode_with_half_full_node) {
  Schema *key_schema = ParseCreateStatement("a bigint");

  GenericComparator<8> comparator(key_schema);
  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  typedef BPlusTree<GenericKey<8>, RID, GenericComparator<8>> BTree;
  BTree tree("foo_pk", bpm, comparator, 3, 2);

  {
    auto rootNode = PageType(bpm, IndexPageType::INTERNAL_PAGE, 3);
    rootNode.toMutableBPlusTreePage()->SetSize(1);
    auto node2 = PageType(bpm, IndexPageType::INTERNAL_PAGE, 3);
    rootNode.toMutableBPlusTreePage()->SetSize(1);
    auto node3 = PageType(bpm, IndexPageType::LEAF_PAGE, 3);
    rootNode.toMutableBPlusTreePage()->SetSize(1);
    {
      BTree::BTreeLockManager bTreeLockManager(&tree, BTree::Mode::remove);

      //  add 3 child page
      //      check all page is rLock
      //      check root mutex lock
      bTreeLockManager.addChild(rootNode);
      EXPECT_EQ(rootNode.getPage()->getLockStatus(), Page::LockStatus::wlock);

      bTreeLockManager.addChild(node2);
      EXPECT_EQ(node2.getPage()->getLockStatus(), Page::LockStatus::wlock);

      bTreeLockManager.addChild(node3);
      EXPECT_EQ(node2.getPage()->getLockStatus(), Page::LockStatus::wlock);
      EXPECT_EQ(rootNode.getPage()->getLockStatus(), Page::LockStatus::wlock);
      EXPECT_EQ(node3.getPage()->getLockStatus(), Page::LockStatus::wlock);

      bTreeLockManager.popChild();
      EXPECT_EQ(node3.getPage()->getLockStatus(), Page::LockStatus::unlock);
    }
    EXPECT_EQ(rootNode.getPage()->getLockStatus(), Page::LockStatus::unlock);
    EXPECT_EQ(node2.getPage()->getLockStatus(), Page::LockStatus::unlock);
    EXPECT_EQ(node3.getPage()->getLockStatus(), Page::LockStatus::unlock);
  }

  //      run out of scope
  //      check all page is unlock
  //      check root mutex unlock
  delete key_schema;
  delete disk_manager;
  delete bpm;
}

}  // namespace bustub