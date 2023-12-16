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
    // LOG_INFO("NewPgImp 索引key %d 未能成功删除",
    // pages_[tmp_frame_id].page_id_);
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
  // 如果找到直接返回
  if (success) {
    // replacer
    replacer_->RecordAccess(tmp_frame_id);
    replacer_->SetEvictable(tmp_frame_id, false);

    pages_[tmp_frame_id].pin_count_++;

    return &pages_[tmp_frame_id];
  }

  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }

  // free_list_ first
  if (!free_list_.empty()) {
    tmp_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
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
  pages_[tmp_frame_id].pin_count_ = 1;
  pages_[tmp_frame_id].is_dirty_ = false;

  // page_table_增加这一页在索引中的信息
  page_table_->Insert(page_id, tmp_frame_id);

  disk_manager_->ReadPage(page_id, pages_[tmp_frame_id].GetData());

  // replacer
  replacer_->RecordAccess(tmp_frame_id);
  replacer_->SetEvictable(tmp_frame_id, false);

  return &pages_[tmp_frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id,
                                           bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> locker(latch_);

  int frame_id; // 待操作的页框号

  /* 查询并修改缓冲池中指定页面的信息 */
  if (page_table_->Find(page_id, frame_id)) {
    /* 如果当前页面pin_count_已经为0直接返回false */
    if (pages_[frame_id].GetPinCount() == 0) {
      return false;
    }

    /* 这里的脏位变换只支持从false到true，从true到false需要与回写配合 */
    if (!pages_[frame_id].IsDirty() && is_dirty) {
      pages_[frame_id].is_dirty_ = true;
    }

    /* 如果当前操作使得pin_count_值为0，需要将对应页面设置为可驱逐。 */
    if (--pages_[frame_id].pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, true);
    }

    return true;
  }

  return false;
}

auto BufferPoolManagerInstance::flushPgImp(page_id_t page_id) -> bool {
  int frame_id; // 待操作的页框号

  /* 如果指定页面存在且脏位为true需要进行回写 */
  if (page_table_->Find(page_id, frame_id)) {
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(),
                               pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }

    return true;
  }

  return false;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> locker(latch_);

  int frame_id; // 待操作的页框号

  /* 如果指定页面存在且脏位为true需要进行回写 */
  if (page_table_->Find(page_id, frame_id)) {
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(),
                               pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }

    return true;
  }

  return false;
}

// 只flush page_table_ 中有的
void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> locker(latch_);

  /* 依次回写所有的脏页 */
  for (int frame_id = 0; frame_id < static_cast<int>(pool_size_); frame_id++) {
    if (pages_[frame_id].GetPageId() != INVALID_PAGE_ID &&
        pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(),
                               pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> locker(latch_);

  int frame_id; // 待操作的页框号

  if (page_table_->Find(page_id, frame_id)) {
    /* 无法删除被固定的页面 */
    if (pages_[frame_id].GetPinCount() > 0) {
      return false;
    }

    /* 如果脏位为true需要进行回写 */
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(),
                               pages_[frame_id].GetData());
    }

    /* 从page_table_和replacer_中删除页面的相关信息 */
    page_table_->Remove(page_id);
    replacer_->Remove(frame_id);

    /* 重置页面的在内存中的缓存和元数据 */
    pages_[frame_id].ResetMemory();
    pages_[frame_id].page_id_ = INVALID_PAGE_ID;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 0;

    /* 将页框归还给空闲链表 */
    free_list_.emplace_back(frame_id);

    /* 重置页面在外存中的数据 */
    DeallocatePage(page_id);

    return true;
  }

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  return next_page_id_++;
}

} // namespace bustub
