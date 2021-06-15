//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      position(0),
      schema(exec_ctx->GetCatalog()->GetTable(plan->TableOid())->schema_),
      child_executor(std::move(child_executor)) {}

void InsertExecutor::Init() {
  tableHeap = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->table_.get();
  if (child_executor != nullptr) {
    child_executor->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (child_executor != nullptr) {
    bool hasNext = child_executor->Next(tuple, rid);
    if (!hasNext) return false;
    tableHeap->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
  } else {
    if (position == plan_->RawValues().size()) {
      return false;
    }
    const std::vector<Value> &values = plan_->RawValues().at(position);
    position += 1;
    *tuple = Tuple(values, &schema);
    tableHeap->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
  }
  std::string table_name = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->name_;
  std::vector<IndexInfo *> indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
  for (auto it = indexes.begin(); it != indexes.end(); ++it) {
    Index *index = (*it)->index_.get();
    index->InsertEntry(*tuple, *rid, exec_ctx_->GetTransaction());
  }
  return true;
}

}  // namespace bustub
