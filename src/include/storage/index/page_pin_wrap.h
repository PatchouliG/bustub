//
// Created by wn on 2021/5/25.
//

#ifndef BUSTUB_PAGE_PIN_WRAP_H
#define BUSTUB_PAGE_PIN_WRAP_H
#include <include/storage/page/b_plus_tree_internal_page.h>
#include <include/storage/page/b_plus_tree_leaf_page.h>
#include <include/storage/page/b_plus_tree_page.h>
#include "buffer/buffer_pool_manager.h"

namespace bustub {
// class PageWrap {
// public:
//  PageWrap(page_id_t pageId, BufferPoolManager *bufferPoolManager)
//      : pageId(pageId), bufferPoolManager(bufferPoolManager) {}
//  virtual ~PageWrap() { bufferPoolManager->UnpinPage(pageId); }
//
// private:
//  page_id_t pageId;
//  Page *page;
//  BufferPoolManager *bufferPoolManager;
//};

INDEX_TEMPLATE_ARGUMENTS
class NodeWrap {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;

 public:
  //  NodeWrap(LeafPage *bPlusTreeLeafPage, BufferPoolManager *bufferPoolManager)
  //      : node_page(bPlusTreeLeafPage), bufferPoolManager(bufferPoolManager) {}
  NodeWrap(page_id_t pageId, BufferPoolManager *bufferPoolManager) : bufferPoolManager(bufferPoolManager) {
    page = bufferPoolManager->FetchPage(pageId);
    is_dirty = false;
    this->page_id = pageId;
    indexPageType = ((BPlusTreePage *)(page->GetData()))->GetPageType();
  }
  //  for new page
  NodeWrap(BufferPoolManager *bufferPoolManager, IndexPageType indexPageType, int max_size,
           page_id_t parent_id = INVALID_PAGE_ID)
      : bufferPoolManager(bufferPoolManager) {
    page = bufferPoolManager->NewPage(&page_id);
    is_dirty = true;
    this->indexPageType = indexPageType;
    if (indexPageType == IndexPageType::LEAF_PAGE) {
      toLeafPage()->Init(page_id, parent_id, max_size);
    } else {
      toInternalPage()->Init(page_id, parent_id, max_size);
    }
  }
  virtual ~NodeWrap() { bufferPoolManager->UnpinPage(page_id, is_dirty); }

  void setIsDirty() { NodeWrap::is_dirty = true; }
  IndexPageType getIndexPageType() const { return indexPageType; }
  LeafPage *toLeafPage() const { return (LeafPage *)(page->GetData()); }
  InternalPage *toInternalPage() const { return (InternalPage *)(page->GetData()); }

 private:
  Page *page;
  page_id_t page_id;
  IndexPageType indexPageType;
  bool is_dirty;
  BufferPoolManager *bufferPoolManager;
};

}  // namespace bustub

#endif  // BUSTUB_PAGE_PIN_WRAP_H
