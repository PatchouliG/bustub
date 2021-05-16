//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

FrameInfo::FrameInfo(frame_id_t frameId) : clock_flag(true), frame_id(frameId) {}
// bool FrameInfo::isPinded() const { return is_pinded; }
// void FrameInfo::setIsPinded(bool isPinded) { is_pinded = isPinded; }
bool FrameInfo::isClockFlag() const { return clock_flag; }
void FrameInfo::setClockFlag(bool clockFlag) { clock_flag = clockFlag; }
frame_id_t FrameInfo::getFrameId() const { return frame_id; }

LRUReplacer::LRUReplacer(size_t num_pages) : max_size(num_pages), next_check_position(0) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  Lock();
  auto res = ClockVictim(frame_id);
  if (!res) {
    res = ClockVictim(frame_id);
  }
  UnLock();
  return res;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  Lock();
  auto position = FindByFrameID(frame_id);
  if (position != frame_infos.end()) {
    if (next_check_position && next_check_position != 0) {
      next_check_position -= 1;
    }
    frame_infos.erase(position);
    //    position->setIsPinded(true);
  } else {
    //    assert(false);
    //      auto free_position = FindFree();
    //      free_position->setFrameId(frame_id);
  }
  UnLock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  Lock();
  auto position = FindByFrameID(frame_id);
  if (position == frame_infos.end()) {
    if (frame_infos.size() == max_size) {
      assert(false);
    }
    frame_infos.push_back(FrameInfo(frame_id));
  } else {
    position->setClockFlag(true);
  }
  UnLock();
}

std::vector<FrameInfo>::iterator LRUReplacer::FindByFrameID(frame_id_t f) {
  for (auto it = frame_infos.begin(); it != frame_infos.end(); it++) {
    if (it->getFrameId() == f) {
      return it;
    }
  }
  return frame_infos.end();
}
void LRUReplacer::EraseAt(size_t position) {
  if (next_check_position >= position && next_check_position != 0) {
    next_check_position -= 1;
  }
  frame_infos.erase(frame_infos.begin() + position);
}

bool LRUReplacer::ClockVictim(frame_id_t *frame_id) {
  if (frame_infos.empty()) {
    return false;
  }
  auto position = next_check_position;
  while (true) {
    auto &r = frame_infos[position];
    if (r.isClockFlag() == false) {
      *frame_id = r.getFrameId();
      //      if (next_check_position == frame_infos.size() - 1) {
      //        next_check_position = 0;
      //      }
      //      frame_infos.erase(frame_infos.begin() + position);
      EraseAt(position);
      return true;
    }
    r.setClockFlag(false);
    position = (position + 1) % frame_infos.size();
    if (position == next_check_position) {
      return false;
    }
  }
}

size_t LRUReplacer::Size() { return frame_infos.size(); }

void LRUReplacer::Lock() { this->mutex.lock(); }
void LRUReplacer::UnLock() { this->mutex.unlock(); }

}  // namespace bustub
