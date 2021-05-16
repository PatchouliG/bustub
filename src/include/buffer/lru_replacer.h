//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {
class FrameInfo {
 public:
  FrameInfo(frame_id_t frameId);
  //  bool isPinded() const;
  //  void setIsPinded(bool isPinded);
  bool isClockFlag() const;
  void setClockFlag(bool clockFlag);
  frame_id_t getFrameId() const;

 private:
  //  bool is_pinded;
  bool clock_flag;
  frame_id_t frame_id;
};

/**
 * LRUReplacer implements the lru replacement policy, which approximates the Least Recently Used policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  void UnLock();
  void Lock();
  bool ClockVictim(frame_id_t *frame_id);
  void EraseAt(size_t position);
  std::vector<FrameInfo>::iterator FindByFrameID(frame_id_t f);

  std::vector<FrameInfo> frame_infos;
  std::mutex mutex;
  size_t max_size;

  size_t next_check_position;
  // TODO(student): implement me!
};

}  // namespace bustub
