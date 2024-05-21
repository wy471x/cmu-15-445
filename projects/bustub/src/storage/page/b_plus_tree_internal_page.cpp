//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  // KeyType key{};
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertByKey(const KeyType &key, const ValueType &value,
                                                 const KeyComparator &comparator,
                                                 BufferPoolManager *buffer_pool_manager) {
  /* 查找插入位置 */
  int insert_pos = 1;
  while (insert_pos < GetSize()) {
    assert(!(comparator(key, array_[insert_pos].first) == 0));  // key不能重复

    if (comparator(key, array_[insert_pos].first) > 0) {
      insert_pos++;
    } else {
      break;
    }
  }

  assert(insert_pos > 0);  // 因为第一个key为无效值所以按照key插入时必须保证在array[0]后面插入

  InsertByIndex(insert_pos, key, value, comparator, buffer_pool_manager);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfDataAndInsertTo(B_PLUS_TREE_INTERNAL_PAGE_TYPE *des_page,
                                                             const KeyType &key, const page_id_t &value,
                                                             const KeyComparator &comparator,
                                                             BufferPoolManager *buffer_pool_manager) {
  /* 整合源页面中的数据和待插入的数据 */
  std::vector<MappingType> tmp_array(GetMaxSize());
  int i = 1;  // 遍历array
  int j = 0;  // 遍历tmp_array
  // [invalid key, 1, 3] & 2 -> [1 2 3]
  while (i < GetMaxSize() && comparator(array_[i].first, key) < 0) {
    tmp_array.at(j) = array_[i];
    i++;
    j++;
  }
  tmp_array.at(j++) = std::make_pair(key, value);
  while (i < GetMaxSize()) {
    tmp_array.at(j) = array_[i];
    i++;
    j++;
  }

  /* 将整合后的数据对半分配到两个子页面中 */
  this->SetSize(1);
  des_page->SetSize(0);
  j = 0;
  for (i = 1; i < GetMinSize(); i++, j++) {
    array_[i] = tmp_array.at(j);
    this->IncreaseSize(1);

    auto child_page =
        reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(tmp_array.at(j).second)->GetData());
    child_page->SetParentPageId(this->GetPageId());
    buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
  }
  for (i = 0; j < GetMaxSize(); i++, j++) {
    des_page->array_[i] = tmp_array.at(j);  // 首个key也被赋有效值
    des_page->IncreaseSize(1);

    auto child_page =
        reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(tmp_array.at(j).second)->GetData());
    child_page->SetParentPageId(des_page->GetPageId());
    buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveByIndex(int index) {
  for (int i = index; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }

  DecreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveByValue(const page_id_t &value) {
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value) {
      RemoveByIndex(i);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertByIndex(int index, const KeyType &key, const ValueType &value,
                                                   const KeyComparator &comparator,
                                                   BufferPoolManager *buffer_pool_manager) {
  array_[index].first = key;
  array_[index].second = value;
  auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(value)->GetData());
  child_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value, true);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetIndexByValue(const ValueType &value) -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }

  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllDataTo(B_PLUS_TREE_INTERNAL_PAGE_TYPE *des_page,
                                                   const KeyComparator &comparator,
                                                   BufferPoolManager *buffer_pool_manager) {
  for (int i = 0, j = des_page->GetSize(); i < GetSize(); i++, j++) {
    des_page->array_[j] = array_[i];
    des_page->IncreaseSize(1);

    auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(array_[i].second)->GetData());
    child_page->SetParentPageId(des_page->GetPageId());
    buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
  }
  this->SetSize(0);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
