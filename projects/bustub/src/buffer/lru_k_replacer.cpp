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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  for (auto it = less_than_k.begin(); it != less_than_k.end(); it++) {
    if (*frame_id == it->GetFrameId() && it->IsEvictable()) {
      less_than_k.erase(it);
      less_than_k_map.erase(less_than_k_map.find(it->GetFrameId()));
      return true;
    }
  }

  for (auto it = greater_than_eq_k.begin(); it != greater_than_eq_k.end(); it++) {
    if (*frame_id == it->GetFrameId() && it->IsEvictable()) {
      greater_than_eq_k.erase(it);
      greater_than_eq_k_map.erase(greater_than_eq_k_map.find(it->GetFrameId()));
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id > replacer_size_, "Invalid frame id!");

  for (auto it = less_than_k.begin(); it != less_than_k.end(); it++) {
    if (frame_id == it->GetFrameId()) {
      it->IncrementUsedCnt();
      if (it->GetUsedCnt() >= k_) {
        greater_than_eq_k.push_back(std::make_unique<Frame>(it));
        greater_than_eq_k_map.insert(std::make_pair(frame_id, greater_than_eq_k));
      } else {
        less_than_k.push_back(std::make_unique<Frame>(it));
      }
      less_than_k.erase(it);
    }
  }

  for (auto it = greater_than_eq_k.begin(); it != greater_than_eq_k.end(); it++) {
    if (frame_id == it->GetFrameId()) {
      it->IncrementUsedCnt();
      greater_than_eq_k.push_back(std::make_unique<Frame>(it));
      greater_than_eq_k.erase(it);
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {}

void LRUKReplacer::Remove(frame_id_t frame_id) {}

auto LRUKReplacer::Size() -> size_t { return 0; }

}  // namespace bustub
