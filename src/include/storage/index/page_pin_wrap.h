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

INDEX_TEMPLATE_ARGUMENTS
class NodePageWrap {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;

 public:
  NodePageWrap(page_id_t pageId, BufferPoolManager *bufferPoolManager) : bufferPoolManager(bufferPoolManager) {
    page = bufferPoolManager->FetchPage(pageId);
    assert(page != nullptr);
    is_dirty = false;
    this->page_id = pageId;
    indexPageType = ((BPlusTreePage *)(page->GetData()))->GetPageType();
  }
  //  for new page
  NodePageWrap(BufferPoolManager *bufferPoolManager, IndexPageType indexPageType, int max_size,
               page_id_t parent_id = INVALID_PAGE_ID)
      : bufferPoolManager(bufferPoolManager) {
    page = bufferPoolManager->NewPage(&page_id);
    //    allocate fail
    assert(page != nullptr);
    is_dirty = true;
    this->indexPageType = indexPageType;
    if (indexPageType == IndexPageType::LEAF_PAGE) {
      toLeafPage()->Init(page_id, parent_id, max_size);
    } else {
      toInternalPage()->Init(page_id, parent_id, max_size);
    }
  }

  NodePageWrap(const NodePageWrap &nodePageWrap)
      : page(nodePageWrap.page),
        page_id(nodePageWrap.page_id),
        indexPageType(nodePageWrap.indexPageType),
        is_dirty(nodePageWrap.is_dirty),
        bufferPoolManager(nodePageWrap.bufferPoolManager) {
    page = bufferPoolManager->FetchPage(page_id);
  }
  virtual ~NodePageWrap() {
    bufferPoolManager->UnpinPage(page_id, is_dirty);
  }

  void setIsDirty() { NodePageWrap::is_dirty = true; }
  IndexPageType getIndexPageType() const { return indexPageType; }
  LeafPage *toLeafPage() const {
    assert(getIndexPageType() == IndexPageType::LEAF_PAGE);
    return (LeafPage *)(page->GetData());
  }
  InternalPage *toInternalPage() const {
    assert(getIndexPageType() == IndexPageType::INTERNAL_PAGE);
    return (InternalPage *)(page->GetData());
  }
  BPlusTreePage *toBPlusTreePage() const { return (BPlusTreePage *)(page->GetData()); }

  page_id_t getPageId() const { return page_id; }
  //  test only
  Page *getPage() const { return page; }

 private:
  Page *page;
  page_id_t page_id;
  IndexPageType indexPageType;
  bool is_dirty;
  BufferPoolManager *bufferPoolManager;
};

}  // namespace bustub

#endif  // BUSTUB_PAGE_PIN_WRAP_H
