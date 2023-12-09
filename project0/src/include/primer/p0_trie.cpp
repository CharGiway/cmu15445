#include "p0_trie.h"
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
namespace bustub {

/*
 * =================================== TrieNode ===================================
 */
TrieNode::TrieNode(char key_char) : key_char_(key_char), is_end_(false) {}

TrieNode::TrieNode(TrieNode &&other_trie_node) noexcept
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



}  // namespace bustub