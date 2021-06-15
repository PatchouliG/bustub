//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iterator(
          exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get()->Begin(exec_ctx_->GetTransaction())) {}

void SeqScanExecutor::Init() {
  auto table_id = plan_->GetTableOid();
  tableHeap = exec_ctx_->GetCatalog()->GetTable(table_id)->table_.get();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (true) {
    if (iterator == tableHeap->End()) {
      return false;
    }
    *tuple = *iterator;
    if (plan_->GetPredicate() == nullptr ||
        plan_->GetPredicate()->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>()) {
      break;
    } else {
      iterator++;
    }
  }
  iterator++;
  return true;
}

}  // namespace bustub
