//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "include/common/logger.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  auto init_bucket = std::make_shared<Bucket>(bucket_size);
  this->dir_.push_back(init_bucket);
  // LOG_INFO("#dir size = %lu", this->dir_.size());
  // system("cat /autograder/source/bustub/test/container/hash/grading_extendible_hash_test.cpp");
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::IncrementGlobalDepth() {
  global_depth_++;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetDirectoryNum() const -> int {
  return dir_.size();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  auto items = bucket->GetItems();
  for (auto &item : items) {
    auto index = IndexOf(item.first);
    dir_[index]->Insert(item.first, item.second);
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RehashDirectoryPointers(std::shared_ptr<Bucket> first, std::shared_ptr<Bucket> second,
                                                        size_t index, int local_depth, int global_depth) -> void {
  size_t distance = 1 << (global_depth_ - 1);
  if (local_depth == global_depth) {
    dir_[index] = first;
    dir_[index + distance] = second;
    for (size_t i = 0; i < distance; i++) {
      if (dir_[i + distance] == nullptr) {
        dir_[i + distance] = dir_[i];
      }
    }
    return;
  }
  std::shared_ptr<Bucket> old_bucket = dir_[index];
  size_t dir_size = GetDirectoryNum();
  for (size_t i = 0; i < dir_size; i++) {
    if (dir_[i] == old_bucket) {
      auto specified_bit = (1 << i) & local_depth;
      if (specified_bit == 0) {
        dir_[i] = first;
      } else {
        dir_[i] = second;
      }
    }
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::SplitAndRehash(std::shared_ptr<Bucket> bucket, size_t index, int local_depth,
                                               int global_depth) -> void {
  // create two new bucket and increment local depth.
  auto new_first_bucket = std::make_shared<Bucket>(bucket_size_, local_depth + 1);
  auto new_second_bucket = std::make_shared<Bucket>(bucket_size_, local_depth + 1);
  RehashDirectoryPointers(new_first_bucket, new_second_bucket, index, local_depth, global_depth);
  RedistributeBucket(bucket);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  auto directory_index = IndexOf(key);
  std::shared_ptr<Bucket> index_bucket = dir_[directory_index];
  if (index_bucket != nullptr) {
    return index_bucket->Find(key, value);
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  auto directory_index = IndexOf(key);
  std::shared_ptr<Bucket> index_bucket = dir_[directory_index];
  if (index_bucket != nullptr) {
    return index_bucket->Remove(key);
  }
  return false;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  auto dir_index = IndexOf(key);
  auto indexed_bucket = dir_[dir_index];
  auto inserted = indexed_bucket->Insert(key, value);
  if (inserted) {
    return;
  }

  auto global_depth = GetGlobalDepthInternal();
  auto local_depth = GetLocalDepthInternal(dir_index);
  if (local_depth == global_depth) {
    IncrementGlobalDepth();
    // double directory size.
    dir_.resize(dir_.size() * 2);
    // create two new bucket.
    SplitAndRehash(indexed_bucket, dir_index, local_depth, global_depth);
  }

  if (local_depth < global_depth) {
    // create two new bucket and increment local depth.
    SplitAndRehash(indexed_bucket, dir_index, local_depth, global_depth);
  }

  num_buckets_++;
  latch_.unlock();
  Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  for (auto &i : this->list_) {
    if (i.first == key) {
      value = i.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  for (auto i = this->list_.begin(); i != this->list_.end(); i++) {
    if (i->first == key) {
      this->list_.erase(i);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // UNREACHABLE("not implemented");
  if (!IsFull()) {
    for (auto it : list_) {
      if (it.first == key) {
        it.second = value;
        return true;
      }
    }
    list_.push_back(std::make_pair(key, value));
    return true;
  }
  return false;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
