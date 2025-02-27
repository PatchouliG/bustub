//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  //  first key is invalid
  //  need test todo
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
int BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::KeyIndex(const KeyType &key,
                                                                       const KeyComparator &comparator) const {
  for (auto i = 0; i < GetSize(); ++i) {
    if (comparator(array[i].first, key) == 0) {
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  //  need test todo
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
// return -1 if not found
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  //  todo need test
  for (auto i = 0; i < GetSize(); ++i) {
    if (array[i].second == value) {
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  //  todo need test
  return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  //  todo need test
  for (auto i = 1; i < GetSize(); ++i) {
    if (comparator(array[i].first, key) > 0) {
      return array[i - 1].second;
    }
  }
  return array[GetSize() - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
// notice: new_value > new_key > old_value
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array[1] = std::pair<KeyType, ValueType>(new_key, new_value);
  //  first key is invalid
  array[0] = std::pair<KeyType, ValueType>(KeyType(), old_value);
  SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  //  need test
  auto position = ValueIndex(old_value);
  assert(position != -1);
  memmove(array + position + 2, array + position + 1, sizeof(MappingType) * (GetSize() - position));
  array[position + 1] = std::make_pair(new_key, new_value);
  SetSize(GetSize() + 1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
// move right half
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  auto position = GetSize() / 2;
  auto move_number = GetSize() - position;
  std::memcpy(recipient->array, array + position, sizeof(MappingType) * (move_number));
  recipient->SetSize(move_number);
  SetSize(position);
  //  todo
  //  recipient->array[0].first = KeyType();
  for (auto i = 0; i < recipient->GetSize(); ++i) {
    auto child_id = recipient->array[i].second;
    auto child = (BPlusTreePage *)(buffer_pool_manager->FetchPage(child_id)->GetData());
    child->SetParentPageId(recipient->GetPageId());
  }

  //
  //    page_id_t pageId;
  //    Page *page = buffer_pool_manager->NewPage(&pageId);
  //    recipient = (BPlusTreeInternalPage *)(page->GetData());
  //    auto move_position = GetSize() / 2;
  //    todo
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  std::memcpy(array + index, array + index + 1, (GetSize() - 1 - index) * sizeof(MappingType));
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() { return INVALID_PAGE_ID; }
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
//  auto firstChild = array[0].second;
  //  recipient->array[recipient->GetSize()] = std::make_pair(middle_key, firstChild);
  auto position = recipient->GetSize();
  std::memmove(&recipient->array[recipient->GetSize()], array, sizeof(MappingType) * GetSize());
  recipient->array[position].first = middle_key;
  recipient->IncreaseSize(GetSize());
  //  set parent to recipient
  for (auto i = 0; i < GetSize(); ++i) {
    auto childPageId = array[i].second;
    Page *page = (buffer_pool_manager->FetchPage(childPageId));
    auto *childPage = (BPlusTreePage *)(page->GetData());
    childPage->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(childPageId, true);
  }
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::MoveLastToFrontOf(BPlusTreeInternalPage *recipient) {
  std::memmove(recipient->array + 1, recipient->array, sizeof(MappingType) * recipient->GetSize());
  recipient->array[0] = array[GetSize() - 1];
  recipient->IncreaseSize(1);
  IncreaseSize(-1);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::MoveFirstToEndOf(BPlusTreeInternalPage *recipient) {
  recipient->array[recipient->GetSize() - 1] = array[0];
  std::memmove(array, array + 1, sizeof(MappingType) * GetSize() - 1);
  recipient->IncreaseSize(1);
  IncreaseSize(-1);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::SetValueAt(int index, const ValueType &value) {
  array[index].second = value;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
std::pair<KeyType, ValueType> BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::PopFirst() {
  assert(GetSize() > GetMinSize());
  auto res = std::make_pair(array[1].first, array[0].second);
  std::memmove(array, array + 1, sizeof(MappingType) * GetSize() - 1);
  array[0].first = KeyType();
  IncreaseSize(-1);
  return res;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::PushLast(std::pair<KeyType, ValueType> value) {
  array[GetSize()] = value;
  IncreaseSize(1);
}
template <typename KeyType, typename ValueType, typename KeyComparator>
std::pair<KeyType, ValueType> BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::PopLast() {
  auto res = array[GetSize() - 1];
  IncreaseSize(-1);
  return res;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::PushFront(std::pair<KeyType, ValueType> value) {
  std::memmove(array + 1, array, sizeof(MappingType) * GetSize() - 1);
  array[0] = value;
  IncreaseSize(1);
}
// template <typename KeyType, typename ValueType, typename KeyComparator>
// void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::merge(BPlusTreeInternalPage *right, KeyType keyType) {
//  auto endPostion=GetSize();
//  right.MoveAllTo(this,bustub::);
//}
// template <typename KeyType, typename ValueType, typename KeyComparator>
// void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::merge(BPlusTreeInternalPage *right, KeyType parentKey,
//                                                                     BufferPoolManager *bufferPoolManager) {
//  auto endPosition = GetSize();
//  right->MoveAllTo(this, bufferPoolManager);
//  array[endPosition].first = parentKey;
//}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
