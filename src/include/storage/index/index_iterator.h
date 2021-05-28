//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  using NodeWrapType = NodePageWrap<KeyType, ValueType, KeyComparator>;

  // you may define your own constructor based on your member variables
  //  todo
//  IndexIterator(const NodePageWrap<KeyType, ValueType, KeyComparator> &nodePageWrap, size_t index);
  IndexIterator(const NodeWrapType &nodePageWrap, BufferPoolManager *bufferPoolManager, int index);
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const {
    return nodePageWrap.toLeafPage()->GetPageId() == itr.nodePageWrap.toLeafPage()->GetPageId() && index == itr.index;
  }

  bool operator!=(const IndexIterator &itr) const { return !operator==(itr); }

 private:
  NodePageWrap<KeyType, ValueType, KeyComparator> nodePageWrap;
  BufferPoolManager *bufferPoolManager;
  int index;

  // add your own private member variables here
};

}  // namespace bustub
