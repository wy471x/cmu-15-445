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
  std::scoped_lock<std::mutex> lock(latch_);

  if (curr_size_ <= 0) {
    return false;
  }

  if (!history_list_.empty()) {
    for (auto it = history_list_.begin(); it != history_list_.end(); it++) {
      if (id_frame_map_.at(*it).IsEvictable()) {
        *frame_id = *it;
        id_frame_map_.erase(*it);
        history_list_.erase(it);
        curr_size_--;
        return true;
      }
    }
  }

  if (!cache_list_.empty()) {
    for (auto it = cache_list_.begin(); it != cache_list_.end(); it++) {
      if (id_frame_map_.at(*it).IsEvictable()) {
        *frame_id = *it;
        id_frame_map_.erase(*it);
        cache_list_.erase(it);
        curr_size_--;
        return true;
      }
    }
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id <= (signed)replacer_size_, "Invalid frame id!");

  auto cur_element = id_frame_map_.find(frame_id);
  if (cur_element == id_frame_map_.end()) {
    history_list_.emplace_back(frame_id);
    id_frame_map_.insert(std::make_pair(frame_id, Frame(std::prev(history_list_.end()), frame_id, current_timestamp_)));
    curr_size_++;
  } else {
    cur_element->second.IncrementUsedCnt();

    if (cur_element->second.GetUsedCnt() < k_) {
      cur_element->second.RecordTimestamp(current_timestamp_);
    } else if (cur_element->second.GetUsedCnt() == k_) {
      cur_element->second.RecordTimestamp(current_timestamp_);
      history_list_.erase(cur_element->second.GetIterator());
      size_t k_timestamp = cur_element->second.GetTimestampList().front();
      auto iter = cache_list_.begin();
      while (iter != cache_list_.end() && k_timestamp > id_frame_map_.at(*iter).GetTimestampList().front()) {
        iter++;
      }
      cur_element->second.SetIterator(cache_list_.insert(iter, frame_id));
    } else {
      auto timestamp_list = cur_element->second.GetTimestampList();
      timestamp_list.pop_front();
      cur_element->second.SetTimestampList(timestamp_list);
      cur_element->second.RecordTimestamp(current_timestamp_);

      cache_list_.erase(cur_element->second.GetIterator());
      size_t k_timestamp = cur_element->second.GetTimestampList().front();
      auto iter = cache_list_.begin();
      while (iter != cache_list_.end() && k_timestamp > id_frame_map_.at(*iter).GetTimestampList().front()) {
        iter++;
      }
      cur_element->second.SetIterator(cache_list_.insert(iter, frame_id));
    }
  }
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id < (signed)replacer_size_, "Invalid frame id!");

  auto cur_element = id_frame_map_.find(frame_id);

  if (cur_element == id_frame_map_.end()) {
    return;
  }

  if (cur_element->second.IsEvictable() && !set_evictable) {
    curr_size_--;
  } else if (!cur_element->second.IsEvictable() && set_evictable) {
    curr_size_++;
  }

  cur_element->second.SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id < (signed)replacer_size_, "Invalid frame id!");

  auto cur_element = id_frame_map_.find(frame_id);

  if (cur_element == id_frame_map_.end() || !cur_element->second.IsEvictable()) {
    return;
  }

  if (cur_element->second.GetUsedCnt() >= k_) {
    cache_list_.erase(cur_element->second.GetIterator());
  } else {
    history_list_.erase(cur_element->second.GetIterator());
  }

  id_frame_map_.erase(cur_element);

  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
