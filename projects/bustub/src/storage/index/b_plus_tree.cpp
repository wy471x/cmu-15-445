#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
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
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
//   bool found = false;
//   Page *page = GetLeafPage(key);
//   auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
//   for (int i = 0; i < leaf_page->GetSize(); i++) {
//     if (comparator_(leaf_page->KeyAt(i), key) == 0) {
//       result->emplace_back(leaf_page->ValueAt(i));
//       found = true;
//     }
//   }

//   buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
//   return found;
// }

// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::GetLeafPage(const KeyType &key) -> Page * {
//   page_id_t next_page_id = root_page_id_;
//   while (true) {
//     Page *page = buffer_pool_manager_->FetchPage(next_page_id);
//     auto tree_pgae = reinterpret_cast<BPlusTreePage *>(page->GetData());
//     if (tree->IsLeafPage()) {
//       return page;
//     }

//     auto internal_page = static_cast<InternalPage *>(tree_page);
//     next_page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
//     for (int i = 1; i < internal_page->GetSize(); i++) {
//       if (comparator_(internal_page->KeyAt(i), key) > 0) {
//         next_page_id = internal_page->ValueAt(i - 1);
//         break;
//       }
//     }
//     buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);
//   }
// }
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
    std::shared_lock<std::shared_mutex> locker(shared_mutex_);

    /* B+树为空 */
    if (root_page_id_ == INVALID_PAGE_ID) {
        return false;
    }

    LeafPage *target_leaf_page = FindLeafPage(key);

    for (int i = 0; i < target_leaf_page->GetSize(); i++) {
        if (comparator_(key, target_leaf_page->KeyAt(i)) == 0) {
            /* 查找成功 */
            result->emplace_back(target_leaf_page->ValueAt(i));
            buffer_pool_manager_->UnpinPage(target_leaf_page->GetPageId(), false);
            return true;
        }
    }

    /* 查找失败 */
    buffer_pool_manager_->UnpinPage(target_leaf_page->GetPageId(), false);
    return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key) -> LeafPage * {
    /* B+树为空 */
    if (root_page_id_ == INVALID_PAGE_ID) {
        return nullptr;
    }

    auto cur_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());

    while (!cur_page->IsLeafPage()) {
        auto internal_page = static_cast<InternalPage *>(cur_page);

        /* 查找下一层待处理的页面 */
        int index = 1;
        while (index < cur_page->GetSize() && comparator_(key, internal_page->KeyAt(index)) >= 0) {
            index++;
        }

        cur_page = reinterpret_cast<BPlusTreePage *>(
                buffer_pool_manager_->FetchPage(internal_page->ValueAt(index - 1))->GetData());
        buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);
    }

    return static_cast<LeafPage *>(cur_page);
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
// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
//   if (IsEmpty()) {
//     Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
//     UpdateRootPageId(1);
//     auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
//     leaf_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
//     leaf_page->SetKeyValueAt(0, key, value);
//     leaf_page->IncreaseSize(1);
//     leaf_page->SetNextPageId(INVALID_PAGE_ID);
//     buffer_pool_manager_->UnpinPage(root_page_id_, true);
//     return true;
//   }

//   Page *page = GetLeafPage(key);
//   auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
//   for (int i = 0; i < leaf_page->GetSize(); i++) {
//     if (comparator_(leaf_page->KeyAt(u), key) == 0) {
//       buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
//       return false;
//     }
//   }
// }

// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::BinarySearchInLeaf(const KeyType &key, LeafPage *node) {
//   int i = 0, j = node->GetSize() - 1;
//   while (i <= j) {
//     int mid = i + (j - i) / 2;
//     if (comparator_(key, node->KeyAt(mid)) > 0) {
//       i = mid + 1;
//     } else if (comparator_(key, node->KeyAt(mid)) < 0) {
//       j = mid - 1;
//     } else {
//       return mid;
//     }
//   }

//   // inserted place
//   return i;
// }
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
    std::unique_lock<std::shared_mutex> locker(shared_mutex_);

    /* B+树为空 */
    if (root_page_id_ == INVALID_PAGE_ID) {
        auto new_root_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&root_page_id_)->GetData());

        /* 初始化新的根页面 */
        new_root_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
        new_root_page->InsertByKey(key, value, comparator_);
        new_root_page->SetNextPageId(INVALID_PAGE_ID);

        UpdateRootPageId(true);

        buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
        return true;
    }

    LeafPage *target_leaf_page = FindLeafPage(key);

    /* key重复 */
    if (!target_leaf_page->InsertByKey(key, value, comparator_)) {
        buffer_pool_manager_->UnpinPage(target_leaf_page->GetPageId(), false);
        return false;
    }

    /* 叶子页面上溢 */
    if (target_leaf_page->GetSize() == target_leaf_page->GetMaxSize()) {
        HandleLeafOverflow(target_leaf_page);
    }

    buffer_pool_manager_->UnpinPage(target_leaf_page->GetPageId(), true);
    return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::HandleLeafOverflow(LeafPage *target_page) {
    if (target_page->IsRootPage()) {
        page_id_t split_page_id;
        auto split_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&split_page_id)->GetData());
        auto new_root_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&root_page_id_)->GetData());

        /* 初始化分裂页面 */
        split_page->Init(split_page_id, root_page_id_, leaf_max_size_);
        target_page->MoveHalfDataTo(split_page);

        /* 初始化新的根页面 */
        new_root_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
        new_root_page->SetKeyAt(0, split_page->KeyAt(0));  // 无任何实际意义的填充值
        new_root_page->SetValueAt(0, target_page->GetPageId());
        new_root_page->SetKeyAt(1, split_page->KeyAt(0));
        new_root_page->SetValueAt(1, split_page->GetPageId());
        new_root_page->IncreaseSize(1);
        target_page->SetParentPageId(root_page_id_);  // 将新根页面设置为旧根页面的父页面

        UpdateRootPageId(false);

        buffer_pool_manager_->UnpinPage(split_page->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
        return;
    }

    page_id_t split_page_id;
    auto split_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&split_page_id)->GetData());
    auto parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(target_page->GetParentPageId())->GetData());

    /* 初始化分裂页面 */
    split_page->Init(split_page_id, parent_page->GetPageId(), leaf_max_size_);
    target_page->MoveHalfDataTo(split_page);

    /* 判断父页面是否上溢 */
    if (parent_page->GetSize() == parent_page->GetMaxSize()) {
        HandleInternalOverflow(parent_page, split_page->KeyAt(0), split_page->GetPageId());
    } else {
        parent_page->InsertByKey(split_page->KeyAt(0), split_page->GetPageId(), comparator_, buffer_pool_manager_);
    }

    buffer_pool_manager_->UnpinPage(split_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::HandleInternalOverflow(InternalPage *target_page, const KeyType &key, const page_id_t &value) {
    if (target_page->IsRootPage()) {
        page_id_t split_page_id;
        auto split_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&split_page_id)->GetData());
        auto new_root_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&root_page_id_)->GetData());

        /* 初始化分裂页面 */
        split_page->Init(split_page_id, root_page_id_, internal_max_size_);
        target_page->MoveHalfDataAndInsertTo(split_page, key, value, comparator_, buffer_pool_manager_);  // split_page首个key暂时有效

        /* 初始化新的根页面 */
        new_root_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
        new_root_page->SetKeyAt(0, split_page->KeyAt(0));  // 无任何实际意义的填充值
        new_root_page->SetValueAt(0, target_page->GetPageId());
        new_root_page->SetKeyAt(1, split_page->KeyAt(0));
        new_root_page->SetValueAt(1, split_page->GetPageId());
        new_root_page->IncreaseSize(1);
        target_page->SetParentPageId(root_page_id_);  // 将新根页面设置为旧根页面的父页面

        UpdateRootPageId(false);

        buffer_pool_manager_->UnpinPage(split_page->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
        return;
    }

    page_id_t split_page_id;
    auto split_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&split_page_id)->GetData());
    auto parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(target_page->GetParentPageId())->GetData());

    /* 初始化分裂页面 */
    split_page->Init(split_page_id, target_page->GetParentPageId(), internal_max_size_);
    target_page->MoveHalfDataAndInsertTo(split_page, key, value, comparator_, buffer_pool_manager_);  // split_page首个key暂时有效

    /* 判断父页面是否上溢 */
    if (parent_page->GetSize() == parent_page->GetMaxSize()) {
        HandleInternalOverflow(parent_page, split_page->KeyAt(0), split_page->GetPageId());
    } else {
        parent_page->InsertByKey(split_page->KeyAt(0), split_page->GetPageId(), comparator_, buffer_pool_manager_);
    }

    buffer_pool_manager_->UnpinPage(split_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}

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
    std::unique_lock<std::shared_mutex> locker(shared_mutex_);

    /* B+树为空 */
    if (root_page_id_ == INVALID_PAGE_ID) {
        return;
    }

    LeafPage *target_leaf_page = FindLeafPage(key);

    /* key不存在 */
    if (!target_leaf_page->RemoveByKey(key, comparator_)) {
        buffer_pool_manager_->UnpinPage(target_leaf_page->GetPageId(), false);
        return;
    }

    if ((target_leaf_page->GetSize() < target_leaf_page->GetMinSize())) {
        if (!target_leaf_page->IsRootPage()) {
            /* 非根叶子页面下溢 */
            HandleLeafUnderflow(target_leaf_page);
        } else if (target_leaf_page->GetSize() == 0) {
            /* 根节点为空 */
            buffer_pool_manager_->UnpinPage(target_leaf_page->GetPageId(), true);
            buffer_pool_manager_->DeletePage(target_leaf_page->GetPageId());
            root_page_id_ = INVALID_PAGE_ID;
            UpdateRootPageId(false);
        } else {
            buffer_pool_manager_->UnpinPage(target_leaf_page->GetPageId(), true);
        }
    } else {
        /* 没有下溢发生 */
        buffer_pool_manager_->UnpinPage(target_leaf_page->GetPageId(), true);
    }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::HandleLeafUnderflow(LeafPage *target_page) {
    int tar_index;
    int bro_index;
    auto parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(target_page->GetParentPageId())->GetData());
    auto bro_page = static_cast<LeafPage *>(GetBrotherPage(parent_page, target_page, tar_index, bro_index));

    /* 从兄弟页面借取 */
    if (bro_page->GetSize() > bro_page->GetMinSize()) {
        if (bro_index < tar_index) {
            /* 从左兄弟借最后一个数据 */
            KeyType bro_last_key = bro_page->KeyAt(bro_page->GetSize() - 1);
            ValueType bro_last_value = bro_page->ValueAt(bro_page->GetSize() - 1);
            bro_page->RemoveByKey(bro_last_key, comparator_);
            target_page->InsertByKey(bro_last_key, bro_last_value, comparator_);
            parent_page->SetKeyAt(tar_index, bro_last_key);
        } else {
            /* 从右兄弟借第一个数据 */
            KeyType bro_first_key = bro_page->KeyAt(0);
            ValueType bro_first_value = bro_page->ValueAt(0);
            bro_page->RemoveByKey(bro_first_key, comparator_);
            target_page->InsertByKey(bro_first_key, bro_first_value, comparator_);
            parent_page->SetKeyAt(bro_index, bro_page->KeyAt(0));
        }

        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(bro_page->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(target_page->GetPageId(), true);
        return;
    }

    /* 将页面向左合并 */
    LeafPage *src_page;
    LeafPage *des_page;
    int src_index;
    if (bro_index < tar_index) {
        /* left_bro <- target */
        src_page = target_page;
        des_page = bro_page;
        src_index = tar_index;
    } else {
        /* target <- right_bro */
        src_page = bro_page;
        des_page = target_page;
        src_index = bro_index;
    }

    src_page->MoveAllDataTo(des_page);
    parent_page->RemoveByIndex(src_index);
    buffer_pool_manager_->UnpinPage(src_page->GetPageId(), true);
    buffer_pool_manager_->DeletePage(src_page->GetPageId());

    if (parent_page->GetSize() < parent_page->GetMinSize()) {
        if (!parent_page->IsRootPage()) {
            /* 非根内部页面下溢 */
            HandleInternalUnderflow(parent_page);
        } else if (parent_page->GetSize() == 1) {
            /* parent_page为根且仅有des_page一个孩子 */
            root_page_id_ = des_page->GetPageId();
            des_page->SetParentPageId(INVALID_PAGE_ID);
            UpdateRootPageId(false);
            buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
            buffer_pool_manager_->DeletePage(parent_page->GetPageId());
        } else {
            buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
        }
    } else {
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    }
    buffer_pool_manager_->UnpinPage(des_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::HandleInternalUnderflow(InternalPage *target_page) {
    /* 从缓冲池获取兄弟页面及相关下标 */
    int tar_index;
    int bro_index;
    auto parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(target_page->GetParentPageId())->GetData());
    auto bro_page = static_cast<InternalPage *>(GetBrotherPage(parent_page, target_page, tar_index, bro_index));

    /* 从兄弟页面借取 */
    if (bro_page->GetSize() > bro_page->GetMinSize()) {
        if (bro_index < tar_index) {
            /* 从左兄弟借最后一个数据 */
            KeyType bro_last_key = bro_page->KeyAt(bro_page->GetSize() - 1);
            page_id_t bro_last_value = bro_page->ValueAt(bro_page->GetSize() - 1);
            bro_page->RemoveByValue(bro_last_value);
            target_page->SetKeyAt(0, parent_page->KeyAt(tar_index));  // 临时填充首个key
            target_page->InsertByIndex(0, bro_last_key, bro_last_value, comparator_, buffer_pool_manager_);
            parent_page->SetKeyAt(tar_index, bro_last_key);
        } else {
            /* 从右兄弟借第一个数据 */
            KeyType bro_first_key = parent_page->KeyAt(bro_index);
            page_id_t bro_first_value = bro_page->ValueAt(0);
            bro_page->RemoveByValue(bro_first_value);
            target_page->InsertByIndex(target_page->GetSize(), bro_first_key, bro_first_value, comparator_, buffer_pool_manager_);
            parent_page->SetKeyAt(bro_index, bro_page->KeyAt(0));
        }

        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(bro_page->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(target_page->GetPageId(), true);
        return;
    }

    /* 将页面向左合并 */
    InternalPage *src_page;
    InternalPage *des_page;
    int src_index;
    if (bro_index < tar_index) {
        /* left_bro <- target */
        src_page = target_page;
        des_page = bro_page;
        src_index = tar_index;
    } else {
        /* target <- right_bro */
        src_page = bro_page;
        des_page = target_page;
        src_index = bro_index;
    }

    src_page->SetKeyAt(0, FindFistKey(src_page));  // 临时填充首个key
    src_page->MoveAllDataTo(des_page, comparator_, buffer_pool_manager_);
    parent_page->RemoveByIndex(src_index);
    buffer_pool_manager_->UnpinPage(src_page->GetPageId(), true);
    buffer_pool_manager_->DeletePage(src_page->GetPageId());

    if (parent_page->GetSize() < parent_page->GetMinSize()) {
        if (!parent_page->IsRootPage()) {
            /* 非根内部页面下溢 */
            HandleInternalUnderflow(parent_page);
        } else if (parent_page->GetSize() == 1) {
            /* parent_page为根且仅有des_page一个孩子 */
            root_page_id_ = des_page->GetPageId();
            des_page->SetParentPageId(INVALID_PAGE_ID);
            UpdateRootPageId(false);
            buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
            buffer_pool_manager_->DeletePage(parent_page->GetPageId());
        } else {
            buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
        }
    } else {
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    }
    buffer_pool_manager_->UnpinPage(des_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetBrotherPage(InternalPage *parent_page, BPlusTreePage *child_page, int &target_index, int &bro_index) -> BPlusTreePage * {
    target_index = parent_page->GetIndexByValue(child_page->GetPageId());

    /* 只有左兄弟 */
    if (target_index == parent_page->GetSize() - 1) {
        auto bro_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(target_index - 1))->GetData());
        bro_index = target_index - 1;
        return bro_page;
    }

    /* 只有右兄弟 */
    if (target_index == 0) {
        auto bro_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(target_index + 1))->GetData());
        bro_index = target_index + 1;
        return bro_page;
    }

    /* 既有左兄弟也有右兄弟 */
    auto lbro_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(target_index - 1))->GetData());
    auto rbro_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(target_index + 1))->GetData());

    /* 左兄弟优先 */
    if (rbro_page->GetSize() > rbro_page->GetMinSize() && lbro_page->GetSize() < lbro_page->GetMinSize()) {
        buffer_pool_manager_->UnpinPage(lbro_page->GetPageId(), false);
        bro_index = target_index + 1;
        return rbro_page;
    }
    buffer_pool_manager_->UnpinPage(rbro_page->GetPageId(), false);
    bro_index = target_index - 1;
    return lbro_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindFistKey(InternalPage *target_page) -> KeyType {
    assert(root_page_id_ != INVALID_PAGE_ID);

    auto cur_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(target_page->ValueAt(0))->GetData());

    while (!cur_page->IsLeafPage()) {
        auto internal_page = static_cast<InternalPage *>(cur_page);
        cur_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(internal_page->ValueAt(0))->GetData());
        buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);
    }
    buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
    return static_cast<LeafPage *>(cur_page)->KeyAt(0);
}


/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
    /* B+树为空 */
    if (root_page_id_ == INVALID_PAGE_ID) {
        return INDEXITERATOR_TYPE();
    }

    /* 循环寻找最左边的叶子页面 */
    auto cur_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
    while (true) {
        if (cur_page->IsLeafPage()) {
            buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
            return INDEXITERATOR_TYPE(cur_page->GetPageId(), 0, buffer_pool_manager_);
        }

        /* 继续查找 */
        page_id_t next_page_id = static_cast<InternalPage *>(cur_page)->ValueAt(0);
        auto next_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(next_page_id)->GetData());
        buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
        cur_page = next_page;
    }
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
    /* B+树为空 */
    if (root_page_id_ == INVALID_PAGE_ID) {
        return INDEXITERATOR_TYPE();
    }
    
    LeafPage *target_leaf_page = FindLeafPage(key);

    int i = 0;
    while (i < target_leaf_page->GetSize() && comparator_(target_leaf_page->KeyAt(i), key) < 0) {
        i++;
    }

    buffer_pool_manager_->UnpinPage(target_leaf_page->GetPageId(), false);
    return INDEXITERATOR_TYPE(target_leaf_page->GetPageId(), i, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
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
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
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
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
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
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
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
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
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
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
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

}  // namespace bustub
