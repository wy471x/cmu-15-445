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
#include "common/logger.h"
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
  std::scoped_lock<std::mutex> locker(latch_);

  if (!free_list_.empty()) {
    auto frame_id = free_list_.front();
    *page_id = AllocatePage();
    page_table_->Insert(*page_id, frame_id);
    free_list_.pop_front();
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    pages_[frame_id].pin_count_++;
    pages_[frame_id].page_id_ = *page_id;
    return &pages_[frame_id];
  }

  frame_id_t frame_id;
  if (!replacer_->Evict(&frame_id)) {
    *page_id = INVALID_PAGE_ID;
    return nullptr;
  }
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
  }
  auto remove_page_id = pages_[frame_id].GetPageId();
  page_table_->Remove(remove_page_id);
  *page_id = AllocatePage();
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_++;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_->Insert(*page_id, frame_id);
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> locker(latch_);
  frame_id_t frame_id;

  if (page_table_->Find(page_id, frame_id)) {
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    pages_[frame_id].pin_count_++;
    return pages_ + frame_id;
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    if (pages_[frame_id].IsDirty()) {
      // LOG_INFO("#page %d data=%s", pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    }
    page_table_->Remove(pages_[frame_id].GetPageId());
  }

  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_++;
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_->Insert(page_id, frame_id);
  return pages_ + frame_id;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> locker(latch_);
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "Invalid page id!");
  frame_id_t frame_id;

  if (page_table_->Find(page_id, frame_id)) {
    if (pages_[frame_id].GetPinCount() > 0) {
      if (!pages_[frame_id].IsDirty() && is_dirty) {
        pages_[frame_id].is_dirty_ = true;
      }
      pages_[frame_id].pin_count_--;
      if (pages_[frame_id].GetPinCount() <= 0) {
        replacer_->SetEvictable(frame_id, true);
      }
      return true;
    }
    return false;
  }

  return false;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> locker(latch_);

  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    return true;
  }
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> locker(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPageId() != INVALID_PAGE_ID && pages_[i].IsDirty()) {
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> locker(latch_);
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "Invalid page id!");

  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    if (pages_[frame_id].GetPinCount() > 0) {
      return false;
    }

    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    }

    page_table_->Remove(page_id);
    replacer_->Remove(frame_id);

    pages_[frame_id].ResetMemory();
    pages_[frame_id].page_id_ = INVALID_PAGE_ID;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 0;

    free_list_.push_back(frame_id);
    DeallocatePage(page_id);
    return true;
  }
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
