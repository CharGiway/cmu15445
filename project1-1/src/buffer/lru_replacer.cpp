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

#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include "buffer/lru_k_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : curr_size_(0), replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  // 先看history，然后再看cache
  for (auto iter = history_list.rbegin(); iter != history_list.rend(); iter++) {
    if ((*iter)->evitable) {
      std::shared_ptr<Node> tmpIt = (*iter);
      *frame_id = tmpIt->frame_id_;
      history_list.erase(tmpIt->iter);
      history_access.erase(tmpIt->frame_id_);
      curr_size_--;
      return true;
    }
  }

  for (auto iter = cache_list.rbegin(); iter != cache_list.rend(); iter++) {
    if ((*iter)->evitable) {
      std::shared_ptr<Node> tmpIt = (*iter);
      *frame_id = tmpIt->frame_id_;
      cache_list.erase(tmpIt->iter);
      cache_access.erase(tmpIt->frame_id_);
      curr_size_--;
      return true;
    }
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw std::invalid_argument("frame_id (" + std::to_string(frame_id) + ") larger than replace_size_");
  }
  current_timestamp_++;
  std::shared_ptr<Node> tmpIt = nullptr;
  if (history_access.find(frame_id) != history_access.end()) {
    tmpIt = history_access[frame_id];
  }
  if (cache_access.find(frame_id) != cache_access.end()) {
    tmpIt = cache_access[frame_id];
  }

  // 如果还没有，那么需要新建一个
  if (tmpIt == nullptr) {
    tmpIt = std::make_shared<Node>(k_);
    tmpIt->frame_id_ = frame_id;
    history_list.push_front(tmpIt);
    tmpIt->iter = history_list.begin();
    history_access[frame_id] = tmpIt;
    curr_size_++;
  }

  tmpIt->UpdateAccess(current_timestamp_);

  // history list部分
  if (tmpIt->access_cout < k_) {
    return;
  }

  // 先插入
  if (tmpIt->access_cout == k_) {
    history_access.erase(tmpIt->frame_id_);
    history_list.erase(tmpIt->iter);

    cache_list.push_back(tmpIt);
    tmpIt->iter = --cache_list.end();
    cache_access[frame_id] = tmpIt;
  }

  // 然后看看要不要调整位置
  cache_list.erase(tmpIt->iter);
  auto iter = cache_list.begin();
  for (; iter != cache_list.end(); iter++) {
    if (tmpIt->timestamp_list_.back() >= (*iter)->timestamp_list_.back()) {
      break;
    }
  }
  tmpIt->iter = cache_list.insert(iter, tmpIt);

  return;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);

  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw std::invalid_argument("frame_id (" + std::to_string(frame_id) + ") larger than replace_size_");
  }

  std::shared_ptr<Node> tmpIt = nullptr;
  if (history_access.find(frame_id) != history_access.end()) {
    tmpIt = history_access[frame_id];
  }
  if (cache_access.find(frame_id) != cache_access.end()) {
    tmpIt = cache_access[frame_id];
  }
  // 没找到，或者evitable无需改变
  if (tmpIt == nullptr || tmpIt->evitable == set_evictable) return;

  tmpIt->evitable = set_evictable;
  if (set_evictable) {
    curr_size_++;
  } else {
    curr_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  bool deleted = remove(frame_id, history_access, history_list);
  if (deleted) {
    return;
  }
  remove(frame_id, cache_access, cache_list);
}

bool LRUKReplacer::remove(frame_id_t frame_id, std::unordered_map<frame_id_t, std::shared_ptr<Node>> &access_,
                          std::list<std::shared_ptr<Node>> &list_) {
  if (access_.find(frame_id) == access_.end()) {
    return false;
  }
  auto tmpIt = access_[frame_id];
  if (!tmpIt->evitable) {
    throw std::invalid_argument("frame_id (" + std::to_string(frame_id) + ") is not evitable");
  }
  list_.erase(tmpIt->iter);
  access_.erase(tmpIt->frame_id_);
  curr_size_--;
  return true;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
