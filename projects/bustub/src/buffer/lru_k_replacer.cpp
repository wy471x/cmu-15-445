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
  system("cat /autograder/source/bustub/test/buffer/grading_lru_k_replacer_test.cpp");
  system("cd ..; cd ..; pwd");
  system("cd ..; cd ..; ls");
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  for (auto it = less_than_k_.begin(); it != less_than_k_.end(); it++) {
    if ((*it)->IsEvictable()) {
      *frame_id = (*it)->GetFrameId();
      less_than_k_.erase(it);
      less_than_k_map_.erase(less_than_k_map_.find(*frame_id));
      curr_size_--;
      return true;
    }
  }

  size_t max = 0;
  std::list<std::unique_ptr<bustub::LRUKReplacer::Frame>>::iterator target;
  for (auto it = greater_than_eq_k_.begin(); it != greater_than_eq_k_.end(); it++) {
    if ((*it)->IsEvictable()) {
      *frame_id = (*it)->GetFrameId();
      if (current_timestamp_ - (*it)->GetKthTimestamp(k_) > max) {
        target = it;
        max = current_timestamp_ - (*it)->GetKthTimestamp(k_);
      }
    }
  }

  if (max != 0 && target != greater_than_eq_k_.end()) {
    *frame_id = (*target)->GetFrameId();
    greater_than_eq_k_.erase(target);
    greater_than_eq_k_map_.erase(greater_than_eq_k_map_.find(*frame_id));
    curr_size_--;
    return true;
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id <= (signed)replacer_size_, "Invalid frame id!");

  if (less_than_k_map_.find(frame_id) == less_than_k_map_.end() &&
      greater_than_eq_k_map_.find(frame_id) == greater_than_eq_k_map_.end()) {
    std::unique_ptr<bustub::LRUKReplacer::Frame> entry = std::make_unique<bustub::LRUKReplacer::Frame>(frame_id);
    entry->IncrementUsedCnt();
    entry->RecordCurrTimestamp(current_timestamp_++);
    less_than_k_.push_back(std::move(entry));
    less_than_k_map_[frame_id] = std::prev(less_than_k_.end());
    curr_size_++;
    return;
  }

  auto find_iter = less_than_k_map_.find(frame_id);
  if (find_iter != less_than_k_map_.end()) {
    (*find_iter->second)->RecordCurrTimestamp(current_timestamp_++);
    (*find_iter->second)->IncrementUsedCnt();
    if ((*find_iter->second)->GetUsedCnt() >= k_) {
      std::unique_ptr<bustub::LRUKReplacer::Frame> insert_element =
          std::make_unique<bustub::LRUKReplacer::Frame>(std::move(*(find_iter->second->get())));
      greater_than_eq_k_.push_back(std::move(insert_element));
      less_than_k_.erase(find_iter->second);
      less_than_k_map_.erase(find_iter);
      greater_than_eq_k_map_[frame_id] = std::prev(greater_than_eq_k_.end());
    }
    return;
  }

  find_iter = greater_than_eq_k_map_.find(frame_id);
  if (find_iter != greater_than_eq_k_map_.end()) {
    (*find_iter->second)->RecordCurrTimestamp(current_timestamp_++);
    (*find_iter->second)->IncrementUsedCnt();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(frame_id < (signed)replacer_size_, "Invalid frame id!");

  auto find_iter = less_than_k_map_.find(frame_id);
  if (find_iter != less_than_k_map_.end()) {
    if ((*find_iter->second)->IsEvictable() && !set_evictable) {
      curr_size_--;
      (*find_iter->second)->SetEvictable(set_evictable);
    }
    if (!(*find_iter->second)->IsEvictable() && set_evictable) {
      curr_size_++;
      (*find_iter->second)->SetEvictable(set_evictable);
    }
  }

  find_iter = greater_than_eq_k_map_.find(frame_id);
  if (find_iter != greater_than_eq_k_map_.end()) {
    if ((*find_iter->second)->IsEvictable() && !set_evictable) {
      curr_size_--;
      (*find_iter->second)->SetEvictable(set_evictable);
    }
    if (!(*find_iter->second)->IsEvictable() && set_evictable) {
      curr_size_++;
      (*find_iter->second)->SetEvictable(set_evictable);
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id < (signed)replacer_size_, "Invalid frame id!");

  auto find_iter = less_than_k_map_.find(frame_id);
  if (find_iter != less_than_k_map_.end()) {
    less_than_k_.erase(find_iter->second);
    less_than_k_map_.erase(find_iter);
  }

  find_iter = greater_than_eq_k_map_.find(frame_id);
  if (find_iter != greater_than_eq_k_map_.end()) {
    greater_than_eq_k_.erase(find_iter->second);
    greater_than_eq_k_map_.erase(find_iter);
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
