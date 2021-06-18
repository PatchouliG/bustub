//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { tableHeap = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->table_.get(); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  bool hasNext = child_executor_->Next(tuple, rid);

  if (!hasNext) {
    return false;
  }
  tableHeap->MarkDelete(*rid, exec_ctx_->GetTransaction());
//  tableHeap->ApplyDelete(*rid, exec_ctx_->GetTransaction());

  std::string table_name = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->name_;
  std::vector<IndexInfo *> indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
  for (auto it = indexes.begin(); it != indexes.end(); ++it) {
    IndexInfo *indexInfo = *it;
    indexInfo->index_.get()->DeleteEntry(*tuple, *rid, exec_ctx_->GetTransaction());
  }

  return true;
}

}  // namespace bustub
