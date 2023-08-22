//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  if (!free_list_.empty()) {
    auto frame_id = free_list_.back();
    *page_id = AllocatePage();
    page_table_->Insert(frame_id, *page_id);
    free_list_.pop_back();
    return &pages_[frame_id];
  }

  if (replacer_->Size() != 0) {
    frame_id_t frame_id;
    replacer_->Evict(&frame_id);
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      page_table_->Remove(pages_[frame_id].GetPageId());
      *page_id = AllocatePage();
      pages_[frame_id].SetPageId(*page_id);
      pages_[frame_id].SetIsDirty(false);
      pages_[frame_id].SetPinCount(0);
      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, true);
    }
    return &pages_[frame_id];
  }
  return nullptr;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  frame_id_t frame_id;

  if (page_table_->Find(page_id, frame_id)) {
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, true);
    pages_[frame_id].SetPinCount(pages_[frame_id].GetPinCount() + 1);
    return &pages_[frame_id];
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    page_table_->Insert(frame_id, page_id);
    free_list_.pop_back();
    return &pages_[frame_id];
  }

  if (replacer_->Size() != 0) {
    replacer_->Evict(&frame_id);
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      page_table_->Remove(pages_[frame_id].GetPageId());
      pages_[frame_id].SetPageId(page_id);
      pages_[frame_id].SetIsDirty(false);
      pages_[frame_id].SetPinCount(0);
      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, true);
    }
    return &pages_[frame_id];
  }

  return nullptr;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  frame_id_t frame_id;

  if (page_table_->Find(page_id, frame_id)) {
    if (pages_[frame_id].GetPinCount() > 0) {
      pages_[frame_id].DecrementPinCount();
      if (pages_[frame_id].GetPinCount() <= 0) {
        replacer_->SetEvictable(frame_id, false);
        pages_[frame_id].SetIsDirty(is_dirty);
      }
      return true;
    }
  }

  return false;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "Invalid page id!");

  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    pages_[frame_id].SetIsDirty(false);
  }
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; i++) {
    disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
    pages_[i].SetIsDirty(false);
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool { 
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "Invalid page id!");

  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    if (pages_[frame_id].GetPinCount() > 0) {
      return false;
    }
    page_table_->Remove(page_id);
    pages_[frame_id].ResetMemory();
    replacer_->Remove(frame_id);
    free_list_.push_back(frame_id);
  }
  return true; 
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
