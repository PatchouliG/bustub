//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <vector>

#include "concurrency/transaction.h"
#include "page_pin_wrap.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include <stack>

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  using NodeWrapType = NodePageWrap<KeyType, ValueType, KeyComparator>;

 public:
  enum class Mode { read, insert, remove };

  class BTreeLockManager {
   public:
    BTreeLockManager(BPlusTree *bPlusTree, Mode mode);
    void addChild(NodeWrapType nodeWrapType);
    NodeWrapType popChild();
    virtual ~BTreeLockManager();

   private:
    BPlusTree *bPlusTree;
    std::stack<NodeWrapType> stack;
    Mode mode;
  };

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  bool IsEmpty() const;

  // Insert a key-value pair into this B+ tree.
  bool Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr);

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  bool GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr);

  // index iterator
  INDEXITERATOR_TYPE begin();
  INDEXITERATOR_TYPE Begin(const KeyType &key);
  INDEXITERATOR_TYPE end();

  bool sizeMoreThanMin(const NodeWrapType &node);
  void Print(BufferPoolManager *bpm) {
    ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
  }

  void Draw(BufferPoolManager *bpm, const std::string &outf) {
    std::ofstream out(outf);
    out << "digraph G {" << std::endl;
    ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
    out << "}" << std::endl;
    out.close();
  }

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);
  // expose for test purpose
  Page *FindLeafPage(const KeyType &key, bool leftMost = false);

 private:
  void StartNewTree(const KeyType &key, const ValueType &value);

  bool InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr);

  void InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                        Transaction *transaction = nullptr);

  //  template <typename N>
  //  N *Split(N *node);

  template <typename N>
  bool CoalesceOrRedistribute(N *node, Transaction *transaction = nullptr);

  template <typename N>
  bool Coalesce(N **neighbor_node, N **node, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent,
                int index, Transaction *transaction = nullptr);

  template <typename N>
  void Redistribute(N *neighbor_node, N *node, int index);

  bool AdjustRoot(BPlusTreePage *node);

  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  NodeWrapType createRoot(bool isFirst);

  int getMaxSizeByType(const BPlusTreePage *bPlusTreePage);
  int getSize(const BPlusTreePage *bPlusTreePage);
  NodeWrapType split(NodeWrapType &node_need_split);
  KeyType minKey(const NodeWrapType &nodeWrapType);
  void updateParentNode(const NodeWrapType &parent, NodeWrapType &a, NodeWrapType &b);

  NodeWrapType findLeaf(const KeyType &key);

  int minSize(const NodeWrapType &node);
  NodeWrapType getRightSibling(const NodeWrapType &node, const NodeWrapType &parent);
  NodeWrapType getLeftSibling(const NodeWrapType &node, const NodeWrapType &parent);
  bool hasLeftSibling(const NodeWrapType &node, const NodeWrapType &parent);
  bool hasRightSibling(const NodeWrapType &node, const NodeWrapType &parent);
  //  void MoveFirstToEndOf(NodeWrapType &left, NodeWrapType &right);
  //  void MoveLastToFrontOf(NodeWrapType &left, NodeWrapType &right);
  void MoveAllTo(NodeWrapType left, NodeWrapType &right);
  KeyType firstKey(const NodeWrapType &node);

  void MoveFirstToEndOf(LeafPage *left, LeafPage *right, InternalPage *parent);
  void MoveLastToFrontOf(LeafPage *left, LeafPage *right, InternalPage *parent);
  void MoveLastToFrontOf(InternalPage *left, InternalPage *right, InternalPage *parent);
  //  return parent array postion to child
  int parentPosition(const InternalPage *parent, page_id_t child_page_id);

  //  todo need delete unused page, update next page id
  //  always merge to left ,delete right
  void mergeLeaf(LeafPage *left, LeafPage *right);
  //  delete index in parent
  void mergeInternal(InternalPage *left, InternalPage *right, InternalPage *parent);

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
};

}  // namespace bustub
