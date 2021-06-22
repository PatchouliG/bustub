//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> l(latch_);
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  txn->GetSharedLockSet()->emplace(rid);
  LockRequest lockRequest(txn->GetTransactionId(), LockMode::SHARED);
  //  get rid in lock table
  auto res = lock_table_.find(rid);
  //  if not exits ,create ,return true
  if (res == lock_table_.end()) {
    LockRequestQueue *lockRequestQueue = new LockRequestQueue;
    lockRequest.setGranted(true);
    lockRequestQueue->request_queue_.push_back(lockRequest);
    lock_table_.insert(std::make_pair(rid, lockRequestQueue));
    return true;
  }
  //  check current lock, if is share return true;
  LockRequestQueue *lockRequestQueue = res->second.get();
  std::optional<LockMode> lockMode = lockRequestQueue->currentLockMode();
  if (!lockMode.has_value()) {
    lockRequest.setGranted(true);
    lockRequestQueue->request_queue_.push_back(lockRequest);
    //    lock_table_.insert(std::make_pair(rid, lockRequestQueue));
    return true;
  } else if (lockMode.value() == LockMode::SHARED) {
    lockRequest.setGranted(true);
    lockRequestQueue->addRequest(lockRequest);
    return true;
  } else {
    lockRequestQueue->addRequest(lockRequest);
    std::unique_lock<std::mutex> m(lockRequestQueue->mutex);
    lockRequestQueue->cv_.wait(
        m, [lockRequestQueue, txn] { return lockRequestQueue->getByTxnId(txn->GetTransactionId()).isStateChange(); });
    LockRequest request = lockRequestQueue->popByTxnId(txn->GetTransactionId());
    return request.isGranted();
  }
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> l(latch_);
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  txn->GetExclusiveLockSet()->emplace(rid);

  LockRequest lockRequest(txn->GetTransactionId(), LockMode::SHARED);
  //  get rid in lock table
  auto res = lock_table_.find(rid);
  //  if not exits ,create ,return true
  if (res == lock_table_.end()) {
    LockRequestQueue *lockRequestQueue = new LockRequestQueue;
    lockRequest.setGranted(true);
    lockRequestQueue->request_queue_.push_back(lockRequest);
    lock_table_.insert(std::make_pair(rid, lockRequestQueue));
    return true;
  }

  //  check current lock, if is share return true;
  LockRequestQueue *lockRequestQueue = res->second.get();
  std::optional<LockMode> lockMode = lockRequestQueue->currentLockMode();
  if (!lockMode.has_value()) {
    lockRequest.setGranted(true);
    lockRequestQueue->request_queue_.push_back(lockRequest);
    //    lock_table_.insert(std::make_pair(rid, lockRequestQueue));
    return true;
  } else {
    lockRequestQueue->addRequest(lockRequest);
    std::unique_lock<std::mutex> m(lockRequestQueue->mutex);
    lockRequestQueue->cv_.wait(
        m, [lockRequestQueue, txn] { return lockRequestQueue->getByTxnId(txn->GetTransactionId()).isStateChange(); });
    LockRequest request = lockRequestQueue->popByTxnId(txn->GetTransactionId());
    return request.isGranted();
  }
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> l(latch_);
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);

  //  get rid in lock table
  //  assert exits
  auto res = lock_table_.find(rid);
  //  if not exits ,create ,return true
  assert(res != lock_table_.end());

  //  remove from queue
  LockRequestQueue *lockRequestQueue = res->second.get();
  LockRequest request = lockRequestQueue->popByTxnId(txn->GetTransactionId());
  request.setLockMode(LockMode::EXCLUSIVE);
  //  check current lock
  std::optional<LockMode> lockMode = lockRequestQueue->currentLockMode();
  //  if not exits ,create ,return true
  if (!lockMode.has_value()) {
    request.setGranted(true);
    lockRequestQueue->addRequest(request);
    return true;
    //  else  add to queue, block
  } else {
    lockRequestQueue->addRequest(request);
    std::unique_lock<std::mutex> m(lockRequestQueue->mutex);
    lockRequestQueue->cv_.wait(
        m, [lockRequestQueue, txn] { return lockRequestQueue->getByTxnId(txn->GetTransactionId()).isStateChange(); });
    request = lockRequestQueue->popByTxnId(txn->GetTransactionId());
    return request.isGranted();
  }
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> l(latch_);

  if (txn->GetState() != TransactionState::ABORTED) {
    txn->SetState(TransactionState::SHRINKING);
  }

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  //  get by rid in table
  auto queueIt = lock_table_.find(rid);
  //  just return
  if (queueIt == lock_table_.end()) {
    return true;
  }
  LockRequestQueue *queue = queueIt->second.get();

  LockRequest lockRequest = queue->popByTxnId(txn->GetTransactionId());

  std::optional<LockMode> lockMode = queue->currentLockMode();
  if (!lockMode.has_value()) {
    queue->grantLock();
  } else if (lockMode.value() == LockMode::SHARED) {
    assert(lockRequest.getLockMode() == LockMode::SHARED);
  } else {
    //    can't be exclude after unlock
    assert(false);
  }
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

bool LockManager::HasCycle(txn_id_t *txn_id) { return false; }

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() { return {}; }

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here
      continue;
    }
  }
}

void LockManager::LockRequestQueue::grantLock() {
  if (request_queue_.size() == 0) {
    return;
  }
  std::unique_lock<std::mutex> l(mutex);
  auto it = request_queue_.begin();
  it->setGranted(true);

  auto mode = it->getLockMode();
  if (mode == LockMode::SHARED) {
    ++it;
    while (it != request_queue_.end()) {
      if (it->getLockMode() == LockMode::SHARED) {
        it->setGranted(true);
      }
    }
  }
  cv_.notify_all();
}

LockManager::LockRequest LockManager::LockRequestQueue::popByTxnId(txn_id_t id) {
  for (auto it = request_queue_.begin(); it != request_queue_.end(); ++it) {
    if (it->getTxnId() == id) {
      LockRequest res = *it;
      request_queue_.erase(it);
      return res;
    }
  }
  assert(false);
}

LockManager::LockRequest LockManager::LockRequestQueue::getByTxnId(txn_id_t id) const {
  for (auto it = request_queue_.begin(); it != request_queue_.end(); ++it) {
    if (it->getTxnId() == id) {
      return *it;
    }
  }
  assert(false);
}
void LockManager::LockRequestQueue::addRequest(LockManager::LockRequest lockRequest) {
  request_queue_.push_back(lockRequest);
}

std::optional<LockManager::LockMode> LockManager::LockRequestQueue::currentLockMode() const {
  for (auto val : request_queue_) {
    if (val.isGranted()) {
      return std::make_optional(val.getLockMode());
    }
  }
  return std::nullopt;
}

}  // namespace bustub
