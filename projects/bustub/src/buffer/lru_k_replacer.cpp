//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  // system("cat /autograder/source/bustub/test/buffer/grading_lru_k_replacer_test.cpp");
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { return false; }

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id <= (signed)replacer_size_, "Invalid frame id!");

  if (less_than_k_map_.find(frame_id) == less_than_k_map_.end() &&
      greater_than_eq_k_map_.find(frame_id) == greater_than_eq_k_map_.end()) {
    auto entry = std::make_unique<bustub::LRUKReplacer::Frame>(frame_id);
    entry->IncrementUsedCnt();
    entry->recordCurrTimestamp(current_timestamp_++);
    less_than_k_.push_back(entry);
    curr_size_++;
  }

  auto find_iter = less_than_k_map_.find(frame_id);
  if (find_iter != less_than_k_map_.end()) {
    auto insert_element = std::make_unique<bustub::LRUKReplacer::Frame>(find_iter->second);
    insert_element->recordCurrTimestamp(current_timestamp_++);
    insert_element->IncrementUsedCnt();
    if (insert_element->GetUsedCnt() >= k_) {
      // todo: update
    } else {
      less_than_k_.push_back(insert_element);
      less_than_k_.erase(find_iter->second);
      less_than_k_map_[frame_id] = std::prev(less_than_k_.end());
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(frame_id < (signed)replacer_size_, "Invalid frame id!");
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id < (signed)replacer_size_, "Invalid frame id!");
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
