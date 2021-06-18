//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)) {
  const Schema *schema = plan_->OutputSchema();
  std::vector<const AbstractExpression *> aggExp;
  for (auto it = schema->GetColumns().begin(); it != schema->GetColumns().end(); ++it) {
    const Column &column = *it;
    const AggregateValueExpression *exp = (const AggregateValueExpression *)(column.GetExpr());
    if (!exp->isGroupByTerm()) {
      aggExp.push_back(exp);
    }
  }
  aht_ = new SimpleAggregationHashTable(aggExp, plan->GetAggregateTypes());
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  while (true) {
    Tuple inputTuple;
    RID inputRid;
    bool hasNext = child_->Next(&inputTuple, &inputRid);
    if (!hasNext) {
      break;
    }
    std::vector<Value> agg;
    std::vector<Value> groupby;
    for (size_t i = 0; i < plan_->GetGroupBys().size(); ++i) {
      groupby.push_back(plan_->GetGroupByAt(i)->Evaluate(&inputTuple, child_->GetOutputSchema()));
    }
    for (size_t i = 0; i < plan_->GetAggregates().size(); ++i) {
      agg.push_back(plan_->GetAggregateAt(i)->Evaluate(&inputTuple, child_->GetOutputSchema()));
    }
    auto ak = AggregateKey();
    ak.group_bys_ = groupby;
    auto av = AggregateValue();
    av.aggregates_ = agg;
    aht_->InsertCombine(ak, av);
  }
  aht_iterator_ = new SimpleAggregationHashTable::Iterator(aht_->Begin());
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (*aht_iterator_ != aht_->End()) {
    tuple

  }
  return true;
}
AggregationExecutor::~AggregationExecutor() {
  delete aht_;
  delete aht_iterator_;
}

}  // namespace bustub
