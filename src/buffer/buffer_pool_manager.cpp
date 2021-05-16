//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  auto res = page_table_.find(page_id);
  frame_id_t frame_id;
  // 1.1    If P exists, pin it and return it immediately.
  if (res != page_table_.end()) {
    frame_id = res->second;
    replacer_->Pin(frame_id);
    auto *p = pages_ + frame_id;
    assert(p->pin_count_ == 0);
    p->pin_count_ = 1;
    return p;
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  if (!free_list_.empty()) {
    auto frame_id_it = free_list_.begin();
    free_list_.erase(frame_id_it);
    frame_id = *frame_id_it;
    //    return pages_ + *free_frame_id;
  } else {
    auto victim_res = replacer_->Victim(&frame_id);
    if (!victim_res) {
      return nullptr;
    }
  }

  auto *page = pages_ + frame_id;
  // 2.     If R is dirty, write it back to the disk.
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->data_);
  }
  // 3.     Delete R from the page table and insert P.
  page_table_.erase(page->page_id_);
  page_table_.insert(std::pair<page_id_t, frame_id_t>(page_id, frame_id));
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  page->ResetMemory();
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  disk_manager_->ReadPage(page_id, page->data_);
  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  auto res = page_table_.find(page_id);
  if (res == page_table_.end()) {
    return false;
  }
  auto *page = pages_ + res->second;
  if (page->GetPinCount() <= 0) {
    return false;
  }
  assert(page->pin_count_ == 1);
  page->pin_count_ = 0;
  page->is_dirty_ = is_dirty;
  replacer_->Unpin(res->second);
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  auto find_res = page_table_.find(page_id);
  if (find_res == page_table_.end()) {
    return false;
  }
  auto frame_id = find_res->second;
  auto *page = pages_ + frame_id;
  disk_manager_->WritePage(page_id, page->data_);
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  *page_id = disk_manager_->AllocatePage();
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  bool pin_check = false;
  for (auto *p = pages_; p < pages_ + pool_size_; ++p) {
    if (p->GetPinCount() == 0) {
      pin_check = true;
      break;
    }
  }
  if (!pin_check) {
    return nullptr;
  }
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = *free_list_.begin();
    free_list_.erase(free_list_.begin());
  } else {
    auto res = replacer_->Victim(&frame_id);
    if (!res) {
      return nullptr;
    }
  }
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  auto *page = pages_ + frame_id;
  if (page->is_dirty_) {
    FlushPageImpl(page->page_id_);
  }
  page_table_.erase(page->GetPageId());

  page->ResetMemory();
  page->pin_count_ = 1;
  page->page_id_ = *page_id;
  page_table_.insert(std::pair<page_id_t, frame_id_t>(*page_id, frame_id));
  // 4.   Set the page ID output parameter. Return a pointer to P.
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  disk_manager_->DeallocatePage(page_id);
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  auto find_res = page_table_.find(page_id);
  if (find_res == page_table_.end()) {
    return true;
  }
  auto *page = pages_ + find_res->second;
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if (page->GetPinCount() > 0) {
    return false;
  }
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  page_table_.erase(page_id);
  page->ResetMemory();
  free_list_.push_back(find_res->second);
  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  for (size_t i = 0; i < pool_size_; ++i) {
    auto &p = pages_[i];
    assert(p.GetPinCount() == 0);
    if (p.IsDirty()) {
      disk_manager_->WritePage(p.GetPageId(), p.data_);
    }
  }
  // You can do it!
}

}  // namespace bustub
