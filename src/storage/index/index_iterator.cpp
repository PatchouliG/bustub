/**
 * index_iterator.cpp
 */
#include <include/storage/index/page_pin_wrap.h>
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
// INDEX_TEMPLATE_ARGUMENTS
// INDEXITERATOR_TYPE::IndexIterator() = default;

// template <typename KeyType, typename ValueType, typename KeyComparator>
// IndexIterator<KeyType, ValueType, KeyComparator>::IndexIterator() {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
  const LeafPage *leafPage = nodePageWrap.toLeafPage();
  return leafPage->GetNextPageId() == INVALID_PAGE_ID && index == leafPage->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  const LeafPage *leafPage = nodePageWrap.toLeafPage();
  return leafPage->GetItem(index);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  assert(!isEnd());
  const LeafPage *leafPage = nodePageWrap.toLeafPage();
  if (leafPage->GetNextPageId() == INVALID_PAGE_ID) {
    index++;
    if (index > leafPage->GetSize()) {
      index = leafPage->GetSize();
    }
  } else if (index != leafPage->GetSize() - 1) {
    index++;
  } else {
    index = 0;
    nodePageWrap = NodeWrapType(leafPage->GetNextPageId(), LatchStatus::readL, bufferPoolManager);
  }
  return *this;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
IndexIterator<KeyType, ValueType, KeyComparator>::IndexIterator(const NodeWrapType &nodePageWrap,
                                                                BufferPoolManager *bufferPoolManager, int index)
    : nodePageWrap(nodePageWrap), bufferPoolManager(bufferPoolManager), index(index) {}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
