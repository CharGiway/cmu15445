#include "p0_trie.h"
#include <memory>

namespace bustub {

/*
 * =================================== TrieNode ===================================
 */
TrieNode::TrieNode(char key_char) : key_char_(key_char_), is_end_(false) {}

TrieNode::TrieNode(TrieNode &&other_trie_node)
    : key_char_(other_trie_node.key_char_),
      is_end_(other_trie_node.is_end_),
      children_(std::move(other_trie_node.children_)) {}

bool TrieNode::HasChildren() const { return !children_.empty(); }

bool TrieNode::HasChild(char key_char) const { return children_.find(key_char) != children_.end(); }

bool TrieNode::IsEndNode() const { return is_end_; }

char TrieNode::GetKeyChar() const { return key_char_; }

void TrieNode::SetEndNode(bool is_end) { is_end_ = is_end; }

std::unique_ptr<TrieNode> *TrieNode::InsertChildNode(char key_char, std::unique_ptr<TrieNode> &&child) {
  if (this->HasChild(key_char) || child->GetKeyChar() != key_char) {
    return nullptr;
  }
  this->children_[key_char] = std::move(child);
  return &this->children_[key_char];
}

std::unique_ptr<TrieNode> *TrieNode::GetChildNode(char key_char) {
  if (!this->HasChild(key_char)) {
    return nullptr;
  }
  return &this->children_[key_char];
}

void TrieNode::RemoveChildNode(char key_char) {
  if (!this->HasChild(key_char)) {
    return;
  }
  this->children_.erase(key_char);
  return;
}

/*
 * =================================== TrieNode ===================================
 */

/*
 * =================================== TrieNodeWithValue ===================================
 */

template <typename T>
TrieNodeWithValue<T>::TrieNodeWithValue(TrieNode &&trieNode, T value)
    : TrieNode(std
               : move(trieNode)),
      value_(value),
      is_end_(true) {}

template <typename T>
TrieNodeWithValue<T>::TrieNodeWithValue(char key_char, T value) : TrieNode(key_char), value_(value), is_end_(true) {}
/*
 * =================================== TrieNodeWithValue ===================================
 */

/*
 * =================================== Trie ===================================
 */
Trie::Trie() : root_(std::make_unique<TrieNode>('\0')) {}

template <typename T>
T Trie::GetValue(const std::string &key, bool *success) {
  if (key.empty()) {
    *success = false;
    return {};
  }
  std::unique_ptr<TrieNode> *tmpRoot = &this->root_;
  for (auto partialKey : key) {
    // 没找到
    if ((*tmpRoot)->children_.find(partialKey) == (*tmpRoot)->children_.end()) {
      *success = false;
      return {};
    }
    tmpRoot = &(*tmpRoot)->children_.find(partialKey);
  }

  // dynamic cast
  auto tmp = dynamic_cast<TrieNodeWithValue<T>>((*tmpRoot).get());
  if (tmp == nullptr) {
    *success = false;
    return {};
  }

  *success = true;
  return tmp->GetValue();
}

/*
 * =================================== Trie ===================================
 */
}  // namespace bustub