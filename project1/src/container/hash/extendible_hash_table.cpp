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
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <list>
#include <memory>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "include/common/logger.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  std::scoped_lock<std::mutex> lock(latch_);
  this->dir_.push_back(std::make_shared<Bucket>(bucket_size_));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOfInK(const K &key,
                                           const int dept) -> size_t {
  return (std::hash<K>()(key) >> (dept - 1)) & 1;
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
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const
    -> int {
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
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  auto index = IndexOf(key);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  size_t index = IndexOf(key);
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  insert(key, value);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::insert(const K &key, const V &value) {

  auto index = IndexOf(key);
  if (!dir_[index]->IsFull()) {
    auto success = dir_[index]->Insert(key, value);
    if (!success) {
      LOG_ERROR("Insert failed");
    }
    return;
  }
  
  // index 如果满了，就需要开始分裂了
  // 如果是等于global_depth_，那么需要扩展一下
  if (dir_[index]->GetDepth() == this->global_depth_) {
    this->global_depth_++;
    auto oldLen = dir_.size();
    auto tmpBucket = std::vector<std::shared_ptr<Bucket>>(oldLen * 2);
    for (size_t i = 0; i < oldLen; i++) {
      tmpBucket[i] = this->dir_[i];
      tmpBucket[i + oldLen] = this->dir_[i];
    }

    // 在将tmpBucket分配给dir_之前释放旧的buckets
    for (size_t i = 0; i < oldLen; i++) {
      this->dir_[i].reset();
    }

    this->dir_ = tmpBucket;
  }

  // 分裂第index个
  std::vector<std::shared_ptr<Bucket>> tmpDir(2);
  for (auto &tmp : tmpDir) {
    tmp = std::make_shared<Bucket>(this->bucket_size_,
                                   this->dir_[index]->GetDepth() + 1);
  }
  auto localItem = this->dir_[index];
  // bool flip = false;
  auto localMask = 1 << (localItem->GetDepth());
  for (size_t i = 0; i < this->dir_.size(); i++) {
    if (dir_[i] != localItem) {
      continue;
    }
    // if (flip) {
    //   this->dir_[i] = tmpDir[0];
    //   flip = !flip;
    // } else {
    //   this->dir_[i] = tmpDir[1];
    //   flip = !flip;
    // }
    if (localMask & i) {
      this->dir_[i] = tmpDir[1];
    } else {
      this->dir_[i] = tmpDir[0];
    }
  }
  this->num_buckets_++;

  for (auto const &tmpItem : localItem->GetItems()) {
    auto tmpIndex = IndexOf(tmpItem.first);
    auto success = this->dir_[tmpIndex]->Insert(tmpItem.first, tmpItem.second);
    if (!success) {
      LOG_ERROR("Insert failed");
    }
  }

  // 最后做一次插入
  // index = IndexOf(key);
  // auto success = dir_[index]->Insert(key, value);
  // if (!success) {
  //   LOG_ERROR("Insert failed");
  //   std::cout << "key is " << key << " index=" << index << "\n";
  // }

  // 漂亮，假设还需要继续扩容，可以使用同一套代码
  insert(key, value);

  return;
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth)
    : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &list_item : list_) {
    if (key == list_item.first) {
      value = list_item.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      it = list_.erase(it); // 删除元素，并将迭代器更新为下一个有效位置
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key,
                                               const V &value) -> bool {
  for (auto &list_item : list_) {
    if (list_item.first == key) {
      list_item.second = value;
      return true;
    }
  }
  if (this->IsFull()) {
    return false;
  }
  list_.push_back(std::pair<K, V>(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

} // namespace bustub
