//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <stack>
#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "include/storage/index/page_pin_wrap.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  NodeWrapType current_node = NodeWrapType(root_page_id_, buffer_pool_manager_);
  while (current_node.getIndexPageType() != IndexPageType::LEAF_PAGE) {
    const InternalPage *internalPage = current_node.toInternalPage();
    auto page_id = internalPage->Lookup(key, comparator_);

    current_node = NodeWrapType(page_id, buffer_pool_manager_);
  }
  const LeafPage *leafPage = current_node.toLeafPage();
  auto index = leafPage->KeyIndex(key, comparator_);
  if (index == -1) {
    return false;
  }
  result->push_back(leafPage->GetItem(index).second);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  //  create root if is invalid
  if (root_page_id_ == INVALID_PAGE_ID) {
    createRoot(true);
  }

  //  find leaf to insert, store parent on path
  std::stack<NodeWrapType> stack;
  NodeWrapType current_node = NodeWrapType(root_page_id_, buffer_pool_manager_);
  while (current_node.getIndexPageType() != IndexPageType::LEAF_PAGE) {
    const InternalPage *internalPage = current_node.toInternalPage();
    auto page_id = internalPage->Lookup(key, comparator_);

    //    todo
    //    if (internalPage->GetSize() == internal_max_size_) {
    stack.push(current_node);
    //    }
    current_node = NodeWrapType(page_id, buffer_pool_manager_);
  }
  //  insert
  assert(current_node.getIndexPageType() == IndexPageType::LEAF_PAGE);
  LeafPage *leafPage = current_node.toMutableLeafPage();
  //  check if exits
  auto res = leafPage->KeyIndex(key, comparator_);
  if (res != -1 && comparator_(leafPage->KeyAt(res), key) == 0) {
    return false;
  }
  leafPage->Insert(key, value, comparator_);

  //  check leaf if need to split

  while (true) {
    auto max_size = getMaxSizeByType(current_node.toBPlusTreePage());
    if (getSize(current_node.toBPlusTreePage()) <= max_size) {
      return true;
    }
    //    split, create new node
    NodeWrapType right_node = split(current_node);
    //    get key
    KeyType min_key = minKey(right_node);
    //    if(isRoot)
    //    create new root ,set child return
    //    insert to parent
    if (current_node.toBPlusTreePage()->IsRootPage()) {
      NodeWrapType root = createRoot(false);
      root.toMutableInternalPage()->PopulateNewRoot(current_node.getPageId(), min_key, right_node.getPageId());
      updateParentNode(root, current_node, right_node);
      //      todo
      //      assert(stack.empty());
      return true;
    } else {
      NodeWrapType parentNodeWrap = stack.top();
      stack.pop();
      assert(parentNodeWrap.getPageId() == current_node.toBPlusTreePage()->GetParentPageId());
      InternalPage *parentNode = parentNodeWrap.toMutableInternalPage();
      parentNode->InsertNodeAfter(current_node.getPageId(), min_key, right_node.getPageId());
      updateParentNode(parentNodeWrap, current_node, right_node);
      current_node = parentNodeWrap;
    }
    //    next loop
  }
  return true;
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  return false;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
// INDEX_TEMPLATE_ARGUMENTS
// template <typename N>
// N *BPLUSTREE_TYPE::Split(N *node) {
//  No
//  return nullptr;
//}
template <typename KeyType, typename ValueType, typename KeyComparator>
typename BPlusTree<KeyType, ValueType, KeyComparator>::NodeWrapType BPlusTree<KeyType, ValueType, KeyComparator>::split(
    BPlusTree::NodeWrapType &node_need_split) {
  NodeWrapType res(buffer_pool_manager_, node_need_split.getIndexPageType(),
                   getMaxSizeByType(node_need_split.toBPlusTreePage()),
                   node_need_split.toBPlusTreePage()->GetParentPageId());
  if (node_need_split.getIndexPageType() == IndexPageType::INTERNAL_PAGE) {
    node_need_split.toMutableInternalPage()->MoveHalfTo(res.toMutableInternalPage(), buffer_pool_manager_);
  } else {
    node_need_split.toMutableLeafPage()->MoveHalfTo(res.toMutableLeafPage());
    node_need_split.toMutableLeafPage()->SetNextPageId(res.getPageId());
  }
  return res;
  //  return bustub::BPlusTree::NodeWrapType(0, nullptr);
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key,
                                                               BPlusTreePage *new_node, Transaction *transaction) {}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return;
  }
  //  find leaf to delete, store parent on path

  std::stack<NodeWrapType> stack;
  NodeWrapType current_node = NodeWrapType(root_page_id_, buffer_pool_manager_);
  while (current_node.getIndexPageType() != IndexPageType::LEAF_PAGE) {
    const InternalPage *internalPage = current_node.toInternalPage();
    auto page_id = internalPage->Lookup(key, comparator_);

    //    todo check size
    stack.push(current_node);
    current_node = NodeWrapType(page_id, buffer_pool_manager_);
  }
  //  check leaf size
  LeafPage *leafPage = current_node.toMutableLeafPage();
  //  delete
  leafPage->RemoveAndDeleteRecord(key, comparator_);

  if (leafPage->GetSize() >= minSize(current_node)) {
    return;
  }
  //  handle root
  if (leafPage->IsRootPage()) {
    assert(leafPage->GetSize() == 1);
    root_page_id_ = INVALID_PAGE_ID;
    return;
  }

  //  handle leaf
  NodeWrapType parent = stack.top();
  //  try redistribute
  if (hasRightSibling(current_node, parent) && sizeMoreThanMin(getRightSibling(current_node, parent))) {
    auto right = getRightSibling(current_node, parent);
    MoveFirstToEndOf(current_node.toMutableLeafPage(), right.toMutableLeafPage(), parent.toMutableInternalPage());
    return;
  }
  if (hasLeftSibling(current_node, parent) && sizeMoreThanMin(getLeftSibling(current_node, parent))) {
    auto left = getLeftSibling(current_node, parent);
    MoveLastToFrontOf(left.toMutableLeafPage(), current_node.toMutableLeafPage(), parent.toMutableInternalPage());
    return;
  }
  //    merge
  page_id_t leftChildPageId;
  int position;
  //  if (hasRightSibling(current_node, parent) && sizeMoreThanMin(getRightSibling(current_node, parent))) {
  if (hasRightSibling(current_node, parent)) {
    NodeWrapType right = getRightSibling(current_node, parent);
    leftChildPageId = current_node.getPageId();
    position = parentPosition(parent.toInternalPage(), right.getPageId());
    mergeLeaf(current_node.toMutableLeafPage(), right.toMutableLeafPage());
  } else {
    NodeWrapType left = getLeftSibling(current_node, parent);
    leftChildPageId = left.getPageId();
    position = parentPosition(parent.toInternalPage(), current_node.getPageId());
    mergeLeaf(left.toMutableLeafPage(), current_node.toMutableLeafPage());
  }

  //  remove parent index
  InternalPage *internalPage = parent.toMutableInternalPage();
  internalPage->Remove(position);
  internalPage->SetValueAt(position - 1, leftChildPageId);

  while (true) {
    current_node = stack.top();
    if (current_node.toInternalPage()->GetSize() >= minSize(current_node)) {
      return;
    }

    //    handle root
    //    need test
    if (current_node.toInternalPage()->IsRootPage()) {
      buffer_pool_manager_->DeletePage(parent.getPageId());
      root_page_id_ = leftChildPageId;
      return;
    }
    stack.pop();
    parent = stack.top();

    if (hasRightSibling(current_node, parent) && sizeMoreThanMin(getRightSibling(current_node, parent))) {
      NodeWrapType right = getRightSibling(current_node, parent);
      InternalPage *rightNode = right.toMutableInternalPage();
      InternalPage *parentNode = parent.toMutableInternalPage();
      auto popRes = rightNode->PopFirst();
      position = parentPosition(parentNode, rightNode->GetPageId());
      auto parentKey = parentNode->KeyAt(position);
      parentNode->SetKeyAt(position, popRes.first);
      current_node.toMutableInternalPage()->PushLast(std::make_pair(parentKey, popRes.second));
      NodeWrapType child = NodeWrapType(popRes.second, buffer_pool_manager_);
      child.toMutableBPlusTreePage()->SetParentPageId(current_node.getPageId());
      return;
    }
    if (hasLeftSibling(current_node, parent) && sizeMoreThanMin(getLeftSibling(current_node, parent))) {
      auto left = getLeftSibling(current_node, parent);
      InternalPage *leftNode = left.toMutableInternalPage();
      InternalPage *parentNode = parent.toMutableInternalPage();
      auto popRes = leftNode->PopLast();
      position = parentPosition(parentNode, current_node.getPageId());
      auto parentKey = parentNode->KeyAt(position);
      parentNode->SetKeyAt(position, popRes.first);
      current_node.toMutableInternalPage()->PushFront(std::make_pair(parentKey, popRes.second));
      NodeWrapType child = NodeWrapType(popRes.second, buffer_pool_manager_);
      child.toMutableBPlusTreePage()->SetParentPageId(current_node.getPageId());
      return;
    }
    // merge
    if (hasRightSibling(current_node, parent)) {
      NodeWrapType right = getRightSibling(current_node, parent);
      InternalPage *rightNode = right.toMutableInternalPage();
      InternalPage *parentNode = parent.toMutableInternalPage();
      InternalPage *leftNode = current_node.toMutableInternalPage();
      position = parentPosition(parentNode, rightNode->GetPageId());
      auto parentKey = parentNode->KeyAt(position);

      rightNode->MoveAllTo(leftNode, parentKey, buffer_pool_manager_);
      parentNode->Remove(position);
      //      must has left
    } else {
      NodeWrapType left = getLeftSibling(current_node, parent);
      InternalPage *leftNode = left.toMutableInternalPage();
      InternalPage *parentNode = parent.toMutableInternalPage();
      InternalPage *rightNode = current_node.toMutableInternalPage();
      position = parentPosition(parentNode, rightNode->GetPageId());
      auto parentKey = parentNode->KeyAt(position);

      rightNode->MoveAllTo(leftNode, parentKey, buffer_pool_manager_);
      parentNode->Remove(position);
    }
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) { return false; }

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  NodeWrapType current_node = NodeWrapType(root_page_id_, buffer_pool_manager_);
  while (current_node.getIndexPageType() != IndexPageType::LEAF_PAGE) {
    const InternalPage *internalPage = current_node.toInternalPage();
    auto page_id = internalPage->ValueAt(0);

    current_node = NodeWrapType(page_id, buffer_pool_manager_);
  }
  return IndexIterator<KeyType, ValueType, KeyComparator>(current_node, buffer_pool_manager_, 0);
  //    return INDEXITERATOR_TYPE();
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  NodeWrapType leaf = findLeaf(key);
  return IndexIterator<KeyType, ValueType, KeyComparator>(leaf, buffer_pool_manager_,
                                                          leaf.toLeafPage()->KeyIndex(key, comparator_));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
  NodeWrapType current_node = NodeWrapType(root_page_id_, buffer_pool_manager_);
  while (current_node.getIndexPageType() != IndexPageType::LEAF_PAGE) {
    const InternalPage *internalPage = current_node.toInternalPage();
    auto page_id = internalPage->ValueAt(internalPage->GetSize() - 1);

    current_node = NodeWrapType(page_id, buffer_pool_manager_);
  }
  return IndexIterator<KeyType, ValueType, KeyComparator>(current_node, buffer_pool_manager_,
                                                          current_node.toLeafPage()->GetSize());
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

template <typename KeyType, typename ValueType, typename KeyComparator>
NodePageWrap<KeyType, ValueType, KeyComparator> BPlusTree<KeyType, ValueType, KeyComparator>::createRoot(bool isFirst) {
  NodeWrapType node_wrap = NodeWrapType(
      buffer_pool_manager_, isFirst ? IndexPageType::LEAF_PAGE : IndexPageType::INTERNAL_PAGE, leaf_max_size_);
  root_page_id_ = node_wrap.getPageId();
  UpdateRootPageId(1);
  return node_wrap;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
int BPlusTree<KeyType, ValueType, KeyComparator>::getMaxSizeByType(const BPlusTreePage *bPlusTreePage) {
  if (bPlusTreePage->IsLeafPage()) {
    return leaf_max_size_;
  } else if (bPlusTreePage->GetPageType() == IndexPageType::INTERNAL_PAGE) {
    return internal_max_size_ + 1;
  }
  assert(false);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
int BPlusTree<KeyType, ValueType, KeyComparator>::getSize(const BPlusTreePage *bPlusTreePage) {
  return bPlusTreePage->GetSize();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType BPlusTree<KeyType, ValueType, KeyComparator>::minKey(const BPlusTree::NodeWrapType &nodeWrapType) {
  if (nodeWrapType.getIndexPageType() == IndexPageType::LEAF_PAGE) {
    return nodeWrapType.toLeafPage()->KeyAt(0);
  } else {
    assert(nodeWrapType.getIndexPageType() == IndexPageType::INTERNAL_PAGE);
    //    todo
    return nodeWrapType.toInternalPage()->KeyAt(0);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::updateParentNode(const BPlusTree::NodeWrapType &parent,
                                                                    BPlusTree::NodeWrapType &a,
                                                                    BPlusTree::NodeWrapType &b) {
  a.toMutableBPlusTreePage()->SetParentPageId(parent.toBPlusTreePage()->GetPageId());
  b.toMutableBPlusTreePage()->SetParentPageId(parent.toBPlusTreePage()->GetPageId());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
typename BPlusTree<KeyType, ValueType, KeyComparator>::NodeWrapType
BPlusTree<KeyType, ValueType, KeyComparator>::findLeaf(const KeyType &key) {
  NodeWrapType current_node = NodeWrapType(root_page_id_, buffer_pool_manager_);
  while (current_node.getIndexPageType() != IndexPageType::LEAF_PAGE) {
    const InternalPage *internalPage = current_node.toInternalPage();
    auto page_id = internalPage->Lookup(key, comparator_);

    current_node = NodeWrapType(page_id, buffer_pool_manager_);
  }
  return current_node;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
int BPlusTree<KeyType, ValueType, KeyComparator>::minSize(const BPlusTree::NodeWrapType &node) {
  //  handle root
  if (node.getPageId() == root_page_id_) {
    if (node.getIndexPageType() == IndexPageType::INTERNAL_PAGE) {
      return 2;
    }
    return 0;
  }
  return node.toBPlusTreePage()->GetMinSize();
}
template <typename KeyType, typename ValueType, typename KeyComparator>
typename BPlusTree<KeyType, ValueType, KeyComparator>::NodeWrapType
BPlusTree<KeyType, ValueType, KeyComparator>::getRightSibling(const BPlusTree::NodeWrapType &node,
                                                              const BPlusTree::NodeWrapType &parent) {
  if (node.getIndexPageType() == IndexPageType::LEAF_PAGE) {
    auto pageId = node.toLeafPage()->GetNextPageId();
    assert(pageId != INVALID_PAGE_ID);
    return NodeWrapType(pageId, buffer_pool_manager_);
  } else {
    const InternalPage *internalPage = parent.toInternalPage();
    auto position = internalPage->ValueIndex(node.getPageId());
    assert(position != internalPage->GetSize() - 1);
    return NodeWrapType(internalPage->ValueAt(position + 1), buffer_pool_manager_);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
typename BPlusTree<KeyType, ValueType, KeyComparator>::NodeWrapType
BPlusTree<KeyType, ValueType, KeyComparator>::getLeftSibling(const BPlusTree::NodeWrapType &node,
                                                             const BPlusTree::NodeWrapType &parent) {
  const InternalPage *internalPage = parent.toInternalPage();
  auto position = internalPage->ValueIndex(node.getPageId());
  assert(position != 0);
  return NodeWrapType(internalPage->ValueAt(position - 1), buffer_pool_manager_);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool BPlusTree<KeyType, ValueType, KeyComparator>::hasLeftSibling(const BPlusTree::NodeWrapType &node,
                                                                  const BPlusTree::NodeWrapType &parent) {
  const InternalPage *page = parent.toInternalPage();
  page_id_t position = page->ValueIndex(node.getPageId());
  return position > 0;
  //  if (node.getIndexPageType() == IndexPageType::LEAF_PAGE) {
  //    return node.toLeafPage()->GetNextPageId() != INVALID_PAGE_ID;
  //  } else {
  //    const InternalPage *page = parent.toInternalPage();
  //    page_id_t position = page->ValueIndex(node.getPageId());
  //    return position != page->GetSize() - 1;
  //  }
}
template <typename KeyType, typename ValueType, typename KeyComparator>
bool BPlusTree<KeyType, ValueType, KeyComparator>::hasRightSibling(const BPlusTree::NodeWrapType &node,
                                                                   const BPlusTree::NodeWrapType &parent) {
  if (node.getIndexPageType() == IndexPageType::LEAF_PAGE) {
    auto next_page_id = node.toLeafPage()->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      return false;
    }
    NodeWrapType next_page = BPlusTree::NodeWrapType(next_page_id, buffer_pool_manager_);
    return next_page.toLeafPage()->GetParentPageId() == node.toLeafPage()->GetParentPageId();
  } else {
    const InternalPage *page = parent.toInternalPage();
    page_id_t position = page->ValueIndex(node.getPageId());
    return position != page->GetSize() - 1;
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::MoveAllTo(BPlusTree::NodeWrapType left,
                                                             BPlusTree::NodeWrapType &right) {
  //
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType BPlusTree<KeyType, ValueType, KeyComparator>::firstKey(const BPlusTree::NodeWrapType &node) {
  assert(node.toBPlusTreePage()->GetSize() >= 1);
  if (node.getIndexPageType() == IndexPageType::LEAF_PAGE) {
    return node.toLeafPage()->KeyAt(0);
  } else {
    return node.toInternalPage()->KeyAt(0);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool BPlusTree<KeyType, ValueType, KeyComparator>::sizeMoreThanMin(const BPlusTree::NodeWrapType &node) {
  if (node.toBPlusTreePage()->IsRootPage()) {
    return node.toBPlusTreePage()->GetSize() > 1;
  }
  if (node.getIndexPageType() == IndexPageType::LEAF_PAGE) {
    return node.toLeafPage()->GetSize() > minSize(node);
  } else {
    return node.toInternalPage()->GetSize() - 1 > minSize(node);
  }
}
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::MoveFirstToEndOf(BPlusTree::LeafPage *left,
                                                                    BPlusTree::LeafPage *right,
                                                                    BPlusTree::InternalPage *parent) {
  right->MoveFirstToEndOf(left);
  auto position = parent->ValueIndex(right->GetPageId());
  parent->SetKeyAt(position, right->KeyAt(0));
}
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::MoveLastToFrontOf(BPlusTree::LeafPage *left,
                                                                     BPlusTree::LeafPage *right,
                                                                     BPlusTree::InternalPage *parent) {
  left->MoveLastToFrontOf(right);
  auto position = parent->ValueIndex(right->GetPageId());
  parent->SetKeyAt(position, right->KeyAt(0));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
int BPlusTree<KeyType, ValueType, KeyComparator>::parentPosition(const BPlusTree::InternalPage *parent,
                                                                 page_id_t child_page_id) {
  return parent->ValueIndex(child_page_id);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::mergeLeaf(BPlusTree::LeafPage *left, BPlusTree::LeafPage *right) {
  right->MoveAllTo(left);
  buffer_pool_manager_->DeletePage(right->GetPageId());
}
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::mergeInternal(BPlusTree::InternalPage *left,
                                                                 BPlusTree::InternalPage *right,
                                                                 BPlusTree::InternalPage *parent) {
  auto position = parentPosition(parent, right->GetPageId());
  auto midKey = parent->KeyAt(position);
  parent->Remove(position);
  right->MoveAllTo(left, midKey, buffer_pool_manager_);
  buffer_pool_manager_->DeletePage(right->GetPageId());
}

}  // namespace bustub
