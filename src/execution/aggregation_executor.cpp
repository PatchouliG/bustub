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
  //  const Schema *schema = plan_->OutputSchema();
  //  plan->GetAggregates()

  //  std::vector<const AbstractExpression *> aggExp;
  //  for (auto it = schema->GetColumns().begin(); it != schema->GetColumns().end(); ++it) {
  //    const Column &column = *it;
  //    const AggregateValueExpression *exp = (const AggregateValueExpression *)(column.GetExpr());
  //    if (!exp->isGroupByTerm()) {
  //      aggExp.push_back(exp);
  //    }
  //  }
  aht_ = new SimpleAggregationHashTable(plan->GetAggregates(), plan->GetAggregateTypes());
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
  bool res = false;
  while (*aht_iterator_ != aht_->End()) {
    auto &key = aht_iterator_->Key().group_bys_;
    auto &value = aht_iterator_->Val().aggregates_;
    //    Tuple valueTuple(value, child_->GetOutputSchema());
    if (plan_->GetHaving() != nullptr && plan_->GetHaving()->EvaluateAggregate(key, value).GetAs<bool>()) {
      //            const Schema *outputSchema = plan_->OutputSchema();
      std::vector<Value> values;
      for (auto it = plan_->OutputSchema()->GetColumns().begin(); it != plan_->OutputSchema()->GetColumns().end();
           ++it) {
        values.push_back(it->GetExpr()->EvaluateAggregate(key, value));
      }
      *tuple = Tuple(values, plan_->OutputSchema());
      res = true;
      aht_iterator_->operator++();
      break;
    }
    aht_iterator_->operator++();
  }
  return res;
}
AggregationExecutor::~AggregationExecutor() {
  delete aht_;
  delete aht_iterator_;
}

}  // namespace bustub
