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
enum LatchStatus { writeL, readL, unlock };
class Internal {
 private:
  Page *page;
  bool is_dirty;
  LatchStatus latchStatus;
  BufferPoolManager *bufferPoolManager;

 public:
  void setIsDirty(bool isDirty) { is_dirty = isDirty; }
  Internal(Page *page, bool isDirty, LatchStatus latchStatus, BufferPoolManager *bufferPoolManager)
      : page(page), is_dirty(isDirty), latchStatus(latchStatus), bufferPoolManager(bufferPoolManager) {
    switch (latchStatus) {
      case writeL:
        page->WLatch();
        break;
      case readL:
        page->RLatch();
        break;
      default:
        break;
    }
  }
  virtual ~Internal() {
    bufferPoolManager->UnpinPage(page->GetPageId(), is_dirty);
    switch (latchStatus) {
      case writeL:
        page->WUnlatch();
        break;
      case readL:
        page->RUnlatch();
        break;
      default:
        break;
    }
  }
};
INDEX_TEMPLATE_ARGUMENTS class NodePageWrap {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;

 public:
  //  for uninitiated
  NodePageWrap() {}
  //  for new page
  NodePageWrap(BufferPoolManager *bufferPoolManager, IndexPageType indexPageType, int max_size,
               const NodePageWrap *parent = nullptr)
      : bufferPoolManager(bufferPoolManager)
  {
    page = bufferPoolManager->NewPage(&page_id);
    assert(page != nullptr);
    internal = std::shared_ptr<Internal>(new Internal(page, false, LatchStatus::unlock, bufferPoolManager));
    this->indexPageType = indexPageType;

    page_id_t parent_id = INVALID_PAGE_ID;
    if (parent != nullptr) {
      parent_id = parent->getPageId();
      parent_internal = parent->parent_internal;
    }
    if (indexPageType == IndexPageType::LEAF_PAGE) {
      toMutableLeafPage()->Init(page_id, parent_id, max_size);
    } else {
      toMutableInternalPage()->Init(page_id, parent_id, max_size);
    }
  }

  NodePageWrap(const NodePageWrap &nodePageWrap)
      : page(nodePageWrap.page),
        page_id(nodePageWrap.page_id),
        indexPageType(nodePageWrap.indexPageType),
        bufferPoolManager(nodePageWrap.bufferPoolManager),
        //        latchStatus(nodePageWrap.latchStatus),
        parent_internal(nodePageWrap.parent_internal),
        internal(nodePageWrap.internal) {}

  NodePageWrap &operator=(const NodePageWrap &other) {
    page = other.page;
    page_id = other.page_id;
    indexPageType = other.indexPageType;
    bufferPoolManager = other.bufferPoolManager;
    parent_internal = other.parent_internal;
    internal = other.internal;
    return *this;
  }

  IndexPageType getIndexPageType() const { return indexPageType; }
  const LeafPage *toLeafPage() const {
    assert(getIndexPageType() == IndexPageType::LEAF_PAGE);
    return (LeafPage *)(page->GetData());
  }
  LeafPage *toMutableLeafPage() {
    assert(getIndexPageType() == IndexPageType::LEAF_PAGE);
    //    this->is_dirty = true;
    internal->setIsDirty(true);
    return (LeafPage *)(page->GetData());
  }
  const InternalPage *toInternalPage() const {
    assert(getIndexPageType() == IndexPageType::INTERNAL_PAGE);
    return (InternalPage *)(page->GetData());
  }
  InternalPage *toMutableInternalPage() {
    assert(getIndexPageType() == IndexPageType::INTERNAL_PAGE);
    internal->setIsDirty(true);
    return (InternalPage *)(page->GetData());
  }
  const BPlusTreePage *toBPlusTreePage() const { return (BPlusTreePage *)(page->GetData()); }
  BPlusTreePage *toMutableBPlusTreePage() {
    internal->setIsDirty(true);
    return (BPlusTreePage *)(page->GetData());
  }

  page_id_t getPageId() const { return page_id; }
  //  test only
  Page *getPage() const { return page; }

  BufferPoolManager *getBufferPoolManager() const { return bufferPoolManager; }

  //  parent null if is root
  NodePageWrap(page_id_t pageId, LatchStatus latchStatus, BufferPoolManager *bufferPoolManager,
               const NodePageWrap *parent = nullptr)
      : bufferPoolManager(bufferPoolManager) {
    page = bufferPoolManager->FetchPage(pageId);
    assert(page != nullptr);
    //    is_dirty = false;
    this->page_id = pageId;
    if (parent != nullptr) {
      parent_internal = (parent->internal);
    }
    indexPageType = ((BPlusTreePage *)(page->GetData()))->GetPageType();
    internal = std::shared_ptr<Internal>(new Internal(page, false, latchStatus, bufferPoolManager));
  }

 private:
  Page *page;
  page_id_t page_id;
  IndexPageType indexPageType;
  BufferPoolManager *bufferPoolManager;

  std::shared_ptr<Internal> parent_internal;
  std::shared_ptr<Internal> internal;
};

}  // namespace bustub

#endif  // BUSTUB_PAGE_PIN_WRAP_H
