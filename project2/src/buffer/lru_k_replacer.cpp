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
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : curr_size_(0), replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  // 先看history，然后再看cache
  for (auto iter = history_list_.rbegin(); iter != history_list_.rend(); iter++) {
    if ((*iter)->evitable_) {
      std::shared_ptr<Node> tmp_it = (*iter);
      *frame_id = tmp_it->frame_id_;
      history_list_.erase(tmp_it->iter_);
      history_access_.erase(tmp_it->frame_id_);
      curr_size_--;
      return true;
    }
  }

  for (auto iter = cache_list_.rbegin(); iter != cache_list_.rend(); iter++) {
    if ((*iter)->evitable_) {
      std::shared_ptr<Node> tmp_it = (*iter);
      *frame_id = tmp_it->frame_id_;
      cache_list_.erase(tmp_it->iter_);
      cache_access_.erase(tmp_it->frame_id_);
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
  std::shared_ptr<Node> tmp_it = nullptr;
  if (history_access_.find(frame_id) != history_access_.end()) {
    tmp_it = history_access_[frame_id];
  }
  if (cache_access_.find(frame_id) != cache_access_.end()) {
    tmp_it = cache_access_[frame_id];
  }

  // 如果还没有，那么需要新建一个
  if (tmp_it == nullptr) {
    tmp_it = std::make_shared<Node>(k_);
    tmp_it->frame_id_ = frame_id;
    history_list_.push_front(tmp_it);
    tmp_it->iter_ = history_list_.begin();
    history_access_[frame_id] = tmp_it;
    curr_size_++;
  }

  tmp_it->UpdateAccess(current_timestamp_);

  // history list部分
  if (tmp_it->access_count_ < k_) {
    return;
  }

  // 先插入
  if (tmp_it->access_count_ == k_) {
    history_access_.erase(tmp_it->frame_id_);
    history_list_.erase(tmp_it->iter_);

    cache_list_.push_back(tmp_it);
    tmp_it->iter_ = --cache_list_.end();
    cache_access_[frame_id] = tmp_it;
  }

  // 然后看看要不要调整位置
  cache_list_.erase(tmp_it->iter_);
  auto iter = cache_list_.begin();
  for (; iter != cache_list_.end(); iter++) {
    if (tmp_it->timestamp_list_.back() >= (*iter)->timestamp_list_.back()) {
      break;
    }
  }
  tmp_it->iter_ = cache_list_.insert(iter, tmp_it);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);

  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw std::invalid_argument("frame_id (" + std::to_string(frame_id) + ") larger than replace_size_");
  }

  std::shared_ptr<Node> tmp_it = nullptr;
  if (history_access_.find(frame_id) != history_access_.end()) {
    tmp_it = history_access_[frame_id];
  }
  if (cache_access_.find(frame_id) != cache_access_.end()) {
    tmp_it = cache_access_[frame_id];
  }
  // 没找到，或者evitable_无需改变
  if (tmp_it == nullptr || tmp_it->evitable_ == set_evictable) {
    return;
  }

  tmp_it->evitable_ = set_evictable;
  if (set_evictable) {
    curr_size_++;
  } else {
    curr_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  bool deleted = DoRemove(frame_id, history_access_, history_list_);
  if (deleted) {
    return;
  }
  DoRemove(frame_id, cache_access_, cache_list_);
}

auto LRUKReplacer::DoRemove(frame_id_t frame_id, std::unordered_map<frame_id_t, std::shared_ptr<Node>> &access_,
                            std::list<std::shared_ptr<Node>> &list_) -> bool {
  if (access_.find(frame_id) == access_.end()) {
    return false;
  }
  auto tmp_it = access_[frame_id];
  if (!tmp_it->evitable_) {
    throw std::invalid_argument("frame_id (" + std::to_string(frame_id) + ") is not evitable_");
  }
  list_.erase(tmp_it->iter_);
  access_.erase(tmp_it->frame_id_);
  curr_size_--;
  return true;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
