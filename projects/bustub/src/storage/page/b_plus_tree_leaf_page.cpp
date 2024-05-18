//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  // KeyType key{};
  return array_[index].first;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertByKey(const KeyType &key, const ValueType &value,
                                             const KeyComparator &comparator) -> bool {
    int initial_len = GetSize();  // 插入数据前的长度
    int insert_pos = 0;           // 插入数据位置

    // 2 -> 1 3
    // 2 -> 1 2 3
    // 2 -> 1
    // 2 -> 3
    while (insert_pos < initial_len) {
        if (comparator(key, array_[insert_pos].first) == 0) {
            return false;  // key不能重复
        }

        if (comparator(key, array_[insert_pos].first) > 0) {
            insert_pos++;
        } else {
            break;
        }
    }

    /* 插入位置后面的元素后移 */
    IncreaseSize(1);
    for (int i = initial_len; i > insert_pos; i--) {
        array_[i] = array_[i - 1];
    }

    /* 插入 */
    array_[insert_pos].first = key;
    array_[insert_pos].second = value;

    return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfDataTo(B_PLUS_TREE_LEAF_PAGE_TYPE *des_page) {
    int initial_len = GetSize();  // 移出数据前的长度

    for (int i = GetMinSize(), j = 0; i < initial_len; i++, j++) {
        des_page->array_[j] = array_[i];
        des_page->IncreaseSize(1);
        this->DecreaseSize(1);
    }

    des_page->SetNextPageId(this->GetNextPageId());
    this->SetNextPageId(des_page->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveByIndex(int index) {
    for (int i = index; i < GetSize() - 1; i++) {
        array_[i] = array_[i + 1];
    }

    DecreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveByKey(const KeyType &key, const KeyComparator &comparator) -> bool {
    for (int i = 0; i < GetSize(); i++) {
        if (comparator(array_[i].first, key) == 0) {
            RemoveByIndex(i);
            return true;
        }
    }

    return false;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllDataTo(B_PLUS_TREE_LEAF_PAGE_TYPE *des_page) {
    for (int i = 0, j = des_page->GetSize(); i < GetSize(); i++, j++) {
        des_page->array_[j] = array_[i];
        des_page->IncreaseSize(1);
    }
    this->SetSize(0);

    des_page->SetNextPageId(this->GetNextPageId());
}


template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
