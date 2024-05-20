/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(LeafPage* page, int index, BufferPoolManager* buffer_pool_manager) : 
page_(page), index_(index), buffer_pool_manager_(buffer_pool_manager){}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(int index, page_id_t page_id, BufferPoolManager* buffer_pool_manager) : 
index_(index), page_id_(page_id), buffer_pool_manager_(buffer_pool_manager){}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(GenericKey key, RID rid, GenericComparator comparator) : 
key_(key), rid_(rid), comparator_(comparator){}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { throw std::runtime_error("unimplemented"); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { throw std::runtime_error("unimplemented"); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {  
    if (IsEnd()) {
        return *this;
    }

    /* 如果index_加一之后溢出需要进行跳页处理 */
    if (++index_ == page_->GetSize()) {
        page_id_ = page_->GetNextPageId();
        if (page_id_ == INVALID_PAGE_ID) {
            buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
            page_ = nullptr;
            index_ = -1;
        } else {
            buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
            page_ = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_id_)->GetData());
            index_ = 0;
        }
    }

    return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
