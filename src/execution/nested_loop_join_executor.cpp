//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor(std::move(left_executor)),
      right_executor(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  RID rid;
  out_is_end = !left_executor->Next(&currentOut, &rid);
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  while (true) {
    if (out_is_end) {
      return false;
    }
    Tuple inertTuple;
    RID inertRid;
    RID outRid;
    bool hasNext = right_executor->Next(&inertTuple, &inertRid);
    if (!hasNext) {
      right_executor->Init();
      out_is_end = !left_executor->Next(&currentOut, &outRid);
      continue;
    }
    if (plan_->Predicate() != nullptr || plan_->Predicate()
                                             ->EvaluateJoin(&currentOut, left_executor.get()->GetOutputSchema(),
                                                            &inertTuple, right_executor.get()->GetOutputSchema())
                                             .GetAs<bool>()) {
      std::vector<Value> values;
      const Schema *left_schema = left_executor.get()->GetOutputSchema();
      const Schema *right_schema = right_executor.get()->GetOutputSchema();
      const Schema *out_schema = plan_->OutputSchema();
      for (uint32_t i = 0; i < out_schema->GetColumnCount(); ++i) {
        std::string col_name = out_schema->GetColumn(i).GetName();
        auto col_idx = left_schema->GetColIdx(col_name);
        if (col_idx != -1) {
          values.push_back(currentOut.GetValue(left_schema, col_idx));
          continue;
        }
        col_idx = right_schema->GetColIdx(col_name);
        assert(col_idx != -1);
        values.push_back(currentOut.GetValue(right_schema, col_idx));
      }
      *tuple = Tuple(values, out_schema);
      return true;
    }
  }
}

}  // namespace bustub
