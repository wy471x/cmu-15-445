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

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  for (auto it = less_than_k_.begin(); it != less_than_k_.end(); it++) {
    if ((*it)->IsEvictable()) {
      auto evict_frame_id = (*it)->GetFrameId();
      *frame_id = evict_frame_id;
      less_than_k_.erase(it);
      auto delete_element = less_than_k_map_.find(evict_frame_id);
      less_than_k_map_.erase(delete_element);
      curr_size_--;
      return true;
    }
  }

  for (auto it = greater_than_eq_k_.begin(); it != greater_than_eq_k_.end(); it++) {
    if ((*it)->IsEvictable()) {
      auto evict_frame_id = (*it)->GetFrameId();
      *frame_id = evict_frame_id;
      greater_than_eq_k_.erase(it);
      auto delete_element = greater_than_eq_k_map_.find(evict_frame_id);
      greater_than_eq_k_map_.erase(delete_element);
      curr_size_--;
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id < (signed)replacer_size_, "Invalid frame id!");

  // if replacer is empty.
  if (curr_size_ <= 0) {
    std::unique_ptr<Frame> entry = std::make_unique<Frame>(frame_id);
    entry->IncrementUsedCnt();
    less_than_k_map_.insert(std::make_pair(frame_id, less_than_k_.insert(less_than_k_.end(), std::move(entry))));
    return;
  }

  for (auto it = less_than_k_.begin(); it != less_than_k_.end(); it++) {
    if (frame_id == (*it)->GetFrameId()) {
      (*it)->IncrementUsedCnt();
      if ((*it)->GetUsedCnt() >= k_) {
        greater_than_eq_k_map_.insert(std::make_pair(
            frame_id,
            greater_than_eq_k_.insert(greater_than_eq_k_.end(), std::make_unique<Frame>(std::move(*(it->get()))))));
        less_than_k_map_.erase(less_than_k_map_.find(frame_id));
      } else {
        less_than_k_.push_back(std::make_unique<Frame>(std::move(*(it->get()))));
      }
      less_than_k_.erase(it);
      return;
    }
    if (it == std::prev(less_than_k_.end())) {
      std::unique_ptr<Frame> entry = std::make_unique<Frame>(frame_id);
      entry->IncrementUsedCnt();
      less_than_k_map_.insert(std::make_pair(frame_id, less_than_k_.insert(less_than_k_.end(), std::move(entry))));
      return;
    }
  }

  for (auto it = greater_than_eq_k_.begin(); it != greater_than_eq_k_.end(); it++) {
    if (frame_id == (*it)->GetFrameId()) {
      (*it)->IncrementUsedCnt();
      greater_than_eq_k_.push_back(std::make_unique<Frame>(std::move(*(it->get()))));
      greater_than_eq_k_.erase(it);
      return;
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(frame_id < (signed)replacer_size_, "Invalid frame id!");

  for (auto &it : less_than_k_) {
    if (frame_id == it->GetFrameId()) {
      if (it->IsEvictable() && !set_evictable) {
        curr_size_--;
      }
      if (!it->IsEvictable() && set_evictable) {
        curr_size_++;
      }
      it->SetEvictable(set_evictable);
      return;
    }
  }

  for (auto &it : greater_than_eq_k_) {
    if (frame_id == it->GetFrameId()) {
      if (it->IsEvictable() && !set_evictable) {
        curr_size_--;
      }
      if (!it->IsEvictable() && set_evictable) {
        curr_size_++;
      }
      it->SetEvictable(set_evictable);
      return;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id < (signed)replacer_size_, "Invalid frame id!");

  for (auto it = less_than_k_.begin(); it != less_than_k_.end(); it++) {
    if (frame_id == (*it)->GetFrameId()) {
      if ((*it)->IsEvictable()) {
        less_than_k_.erase(it);
        less_than_k_map_.erase(less_than_k_map_.find(frame_id));
        curr_size_--;
        return;
      }
      BUSTUB_ASSERT((*it)->IsEvictable(), "Illegal operatrion: Remove on non-evictable frame!");
    }
  }

  for (auto it = greater_than_eq_k_.begin(); it != greater_than_eq_k_.end(); it++) {
    if (frame_id == (*it)->GetFrameId()) {
      if ((*it)->IsEvictable()) {
        greater_than_eq_k_.erase(it);
        greater_than_eq_k_map_.erase(greater_than_eq_k_map_.find(frame_id));
        curr_size_--;
        return;
      }
      BUSTUB_ASSERT((*it)->IsEvictable(), "Illegal operatrion: Remove on non-evictable frame!");
    }
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
