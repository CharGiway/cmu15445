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

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include <memory>
#include <string>

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size,
                                                     DiskManager *disk_manager,
                                                     size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager),
      log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool
  // manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished "
  //     "implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> locker(latch_);

  // whether free space
  if (free_list_.empty() && replacer_->Size() == 0) {
    page_id = nullptr;
    return nullptr;
  }
  frame_id_t tmp_frame_id = -1;
  // free_list_ first
  if (!free_list_.empty()) {
    tmp_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    bool success = replacer_->Evict(&tmp_frame_id);
    if (!success) {
      LOG_ERROR("replacer_->Evict faild");
      page_id = nullptr;
      return nullptr;
    }
  }

  *page_id = AllocatePage();

  // if replacement frame dirty
  if (pages_[tmp_frame_id].IsDirty()) {
    bool success = flushPgImp(pages_[tmp_frame_id].page_id_);
    if (!success) {
      LOG_ERROR("NewPgImp flushPgImp page_id=%d 未能成功",
                pages_[tmp_frame_id].page_id_);
      return nullptr;
    }
  }

  // page_table_删除这一页在索引中的信息
  bool success = page_table_->Remove(pages_[tmp_frame_id].page_id_);
  if (!success) {
    // LOG_INFO("NewPgImp 索引key %d 未能成功删除", pages_[tmp_frame_id].page_id_);
  }

  // reset metadata
  pages_[tmp_frame_id].page_id_ = *page_id;
  pages_[tmp_frame_id].ResetMemory();
  pages_[tmp_frame_id].pin_count_ = 1;
  pages_[tmp_frame_id].is_dirty_ = false;

  // page_table_增加这一页在索引中的信息
  page_table_->Insert(*page_id, tmp_frame_id);

  // replacer
  replacer_->RecordAccess(tmp_frame_id);
  replacer_->SetEvictable(tmp_frame_id, false);

  return &pages_[tmp_frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> locker(latch_);

  frame_id_t tmp_frame_id = -1;
  bool success = page_table_->Find(page_id, tmp_frame_id);
  if (!success) {
    if (free_list_.empty() && replacer_->Size() == 0) {
      return nullptr;
    }

    // free_list_ first
    if (!free_list_.empty()) {
      tmp_frame_id = free_list_.front();
      free_list_.pop_front();
    }
    if (free_list_.empty()) {
      bool success = replacer_->Evict(&tmp_frame_id);
      if (!success) {
        LOG_ERROR("replacer_->Evict faild");
        return nullptr;
      }
    }

    if (pages_[tmp_frame_id].IsDirty() &&
        pages_[tmp_frame_id].page_id_ != INVALID_PAGE_ID) {
      success = flushPgImp(pages_[tmp_frame_id].page_id_);
      if (!success) {
        LOG_ERROR("FetchPgImp flushPgImp page_id=%d 未能成功", page_id);
        return nullptr;
      }
    }

    // page_table_删除这一页在索引中的信息
    success = page_table_->Remove(pages_[tmp_frame_id].page_id_);
    if (!success) {
      // LOG_INFO("FetchPgImp 索引key %d 未能成功删除",
      //          pages_[tmp_frame_id].page_id_);
    }
    // reset metadata
    pages_[tmp_frame_id].page_id_ = page_id;
    pages_[tmp_frame_id].ResetMemory();
    pages_[tmp_frame_id].pin_count_ = 0;
    pages_[tmp_frame_id].is_dirty_ = false;

    // page_table_增加这一页在索引中的信息
    page_table_->Insert(page_id, tmp_frame_id);

    disk_manager_->ReadPage(page_id, pages_[tmp_frame_id].GetData());
  }

  // replacer
  replacer_->RecordAccess(tmp_frame_id);
  replacer_->SetEvictable(tmp_frame_id, false);

  pages_[tmp_frame_id].pin_count_++;

  return &pages_[tmp_frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id,
                                           bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> locker(latch_);

  frame_id_t tmp_frame_id = -1;
  bool success = page_table_->Find(page_id, tmp_frame_id);
  if (!success) {
    return false;
  }
  if (pages_[tmp_frame_id].pin_count_ == 0) {
    return false;
  }
  pages_[tmp_frame_id].pin_count_--;
  if (pages_[tmp_frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(tmp_frame_id, true);
  }

  pages_[tmp_frame_id].is_dirty_ = is_dirty;

  return true;
}

auto BufferPoolManagerInstance::flushPgImp(page_id_t page_id) -> bool {
  frame_id_t tmp_frame_id = -1;
  bool success = page_table_->Find(page_id, tmp_frame_id);
  if (!success) {
    return false;
  }

  auto page_data = pages_[tmp_frame_id].GetData();
  disk_manager_->WritePage(page_id, page_data);
  pages_[tmp_frame_id].is_dirty_ = false;
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> locker(latch_);

  frame_id_t tmp_frame_id = -1;
  bool success = page_table_->Find(page_id, tmp_frame_id);
  if (!success) {
    return false;
  }

  auto page_data = pages_[tmp_frame_id].GetData();
  disk_manager_->WritePage(page_id, page_data);
  pages_[tmp_frame_id].is_dirty_ = false;
  return true;
}

// 只flush page_table_ 中有的
void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> locker(latch_);

  for (size_t index = 0; index < pool_size_; index++) {
    bool success = FlushPage(pages_[index].GetPageId());
    if (!success) {
      // LOG_INFO("%s can not flush in page_table_, skip",
      //          std::to_string(pages_[index].GetPageId()).c_str());
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> locker(latch_);

  frame_id_t tmp_frame_id = -1;
  bool success = page_table_->Find(page_id, tmp_frame_id);
  if (!success) {
    return true;
  }

  if (pages_[tmp_frame_id].pin_count_ > 0) {
    return false;
  }
  Page *tmpPage = &pages_[tmp_frame_id];
  success = page_table_->Remove(tmp_frame_id);
  if (!success) {
    LOG_ERROR("索引删除 frame id %d 失败", tmp_frame_id);
    return false;
  }
  // stop tracking
  replacer_->Remove(tmp_frame_id);
  free_list_.push_back(tmp_frame_id);

  tmpPage->ResetMemory();
  tmpPage->pin_count_ = 0;
  tmpPage->is_dirty_ = false;
  tmpPage->page_id_ = INVALID_PAGE_ID;

  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  return next_page_id_++;
}

} // namespace bustub
