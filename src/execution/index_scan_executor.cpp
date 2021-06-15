//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx) {}

void IndexScanExecutor::Init() {
  index = (BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *)(exec_ctx_->GetCatalog()->GetIndex(
      plan_->GetIndexOid()));
  it = new IndexIterator<GenericKey<8>, RID, GenericComparator<8>>(index->GetBeginIterator());
}
IndexScanExecutor::~IndexScanExecutor() { delete it; }

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (true) {
    if (*it == index->GetEndIterator()) {
      return false;
    }
    *tuple = Tuple(it->operator*().second);
    if (plan_->GetPredicate() == nullptr ||
        plan_->GetPredicate()->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>()) {
      break;
    }
    it->operator++();
  }
  it->operator++();
  return true;
}

}  // namespace bustub
