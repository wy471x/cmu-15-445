//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_trie.h
//
// Identification: src/include/primer/p0_trie.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rwlatch.h"

namespace bustub {

template <typename Derived, typename Base>
std::unique_ptr<Derived> DynamicPtrCast(std::unique_ptr<Base> &&base) noexcept;

/**
 * TrieNode is a generic container for any node in Trie.
 */
class TrieNode {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie Node object with the given key char.
   * is_end_ flag should be initialized to false in this constructor.
   *
   * @param key_char Key character of this trie node
   */
  explicit TrieNode(char key_char) {
    // LOG_INFO("TrieNode constructor: key = %c", key_char);
    this->key_char_ = key_char;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Move constructor for trie node object. The unique pointers stored
   * in children_ should be moved from other_trie_node to new trie node.
   *
   * @param other_trie_node Old trie node.
   */
  TrieNode(TrieNode &&other_trie_node) noexcept {
    this->key_char_ = other_trie_node.key_char_;
    for (auto it = other_trie_node.children_.begin(); it != other_trie_node.children_.end(); ++it) {
      this->children_[it->first] = std::move(it->second);
    }
    this->is_end_ = other_trie_node.is_end_;
  }

  /**
   * @brief Destroy the TrieNode object.
   */
  virtual ~TrieNode() = default;

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has a child node with specified key char.
   *
   * @param key_char Key char of child node.
   * @return True if this trie node has a child with given key, false otherwise.
   */
  bool HasChild(char key_char) const {
    // LOG_INFO("#TrieNode:HasChild: key_char = %c", key_char);
    return this->children_.count(key_char) == 1;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has any children at all. This is useful
   * when implementing 'Remove' functionality.
   *
   * @return True if this trie node has any child node, false if it has no child node.
   */
  bool HasChildren() const { return !this->children_.empty(); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node is the ending character of a key string.
   *
   * @return True if is_end_ flag is true, false if is_end_ is false.
   */
  bool IsEndNode() const { return this->is_end_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Return key char of this trie node.
   *
   * @return key_char_ of this trie node.
   */
  char GetKeyChar() const { return this->key_char_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert a child node for this trie node into children_ map, given the key char and
   * unique_ptr of the child node. If specified key_char already exists in children_,
   * return nullptr. If parameter `child`' s key char is different than parameter
   * `key_char`, return nullptr.
   *
   * Note that parameter `child` is rvalue and should be moved when it is
   * inserted into children_map.
   *
   * The return value is a pointer to unique_ptr because pointer to unique_ptr can access the
   * underlying data without taking ownership of the unique_ptr. Further, we can set the return
   * value to nullptr when error occurs.
   *
   * @param key Key of child node
   * @param child Unique pointer created for the child node. This should be added to children_ map.
   * @return Pointer to unique_ptr of the inserted child node. If insertion fails, return nullptr.
   */
  std::unique_ptr<TrieNode> *InsertChildNode(char key_char, std::unique_ptr<TrieNode> &&child) {
    if (key_char != child->GetKeyChar() || this->children_.count(key_char) == 1) {
      return nullptr;
    }
    this->children_[key_char] = std::move(child);
    return &this->children_[key_char];
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the child node given its key char. If child node for given key char does
   * not exist, return nullptr.
   *
   * @param key Key of child node
   * @return Pointer to unique_ptr of the child node, nullptr if child
   *         node does not exist.
   */
  std::unique_ptr<TrieNode> *GetChildNode(char key_char) {
    if (this->children_.count(key_char) == 0) {
      return nullptr;
    }
    return &this->children_[key_char];
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove child node from children_ map.
   * If key_char does not exist in children_, return immediately.
   *
   * @param key_char Key char of child node to be removed
   */
  void RemoveChildNode(char key_char) {
    if (this->children_.count(key_char) == 0) {
      return;
    }

    for (auto it = this->children_.begin(); it != this->children_.end();) {
      if (it->first == key_char) {
        it = this->children_.erase(it);
        return;
      } else {
        ++it;
      }
    }
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Set the is_end_ flag to true or false.
   *
   * @param is_end Whether this trie node is ending char of a key string
   */
  void SetEndNode(bool is_end) { this->is_end_ = is_end; }

 protected:
  /** Key character of this trie node */
  char key_char_;
  /** whether this node marks the end of a key */
  bool is_end_{false};
  /** A map of all child nodes of this trie node, which can be accessed by each
   * child node's key char. */
  std::unordered_map<char, std::unique_ptr<TrieNode>> children_;
};

/**
 * TrieNodeWithValue is a node that marks the ending of a key, and it can
 * hold a value of any type T.
 */
template <typename T>
class TrieNodeWithValue : public TrieNode {
 private:
  /* Value held by this trie node. */
  T value_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue object from a TrieNode object and specify its value.
   * This is used when a non-terminal TrieNode is converted to terminal TrieNodeWithValue.
   *
   * The children_ map of TrieNode should be moved to the new TrieNodeWithValue object.
   * Since it contains unique pointers, the first parameter is a rvalue reference.
   *
   * You should:
   * 1) invoke TrieNode's move constructor to move data from TrieNode to
   * TrieNodeWithValue.
   * 2) set value_ member variable of this node to parameter `value`.
   * 3) set is_end_ to true
   *
   * @param trieNode TrieNode whose data is to be moved to TrieNodeWithValue
   * @param value
   */
  TrieNodeWithValue(TrieNode &&trieNode, T value) : TrieNode(std::move(trieNode)) { this->value_ = value; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue. This is used when a new terminal node is constructed.
   *
   * You should:
   * 1) Invoke the constructor for TrieNode with the given key_char.
   * 2) Set value_ for this node.
   * 3) set is_end_ to true.
   *
   * @param key_char Key char of this node
   * @param value Value of this node
   */
  TrieNodeWithValue(char key_char, T value) : TrieNode(key_char) { this->value_ = value; }

  /**
   * @brief Destroy the Trie Node With Value object
   */
  ~TrieNodeWithValue() override = default;

  /**
   * @brief Get the stored value_.
   *
   * @return Value of type T stored in this node
   */
  T GetValue() const { return value_; }
};

/**
 * Trie is a concurrent key-value store. Each key is a string and its corresponding
 * value can be any type.
 */
class Trie {
 private:
  /* Root node of the trie */
  std::unique_ptr<TrieNode> root_;
  /* Read-write lock for the trie */
  ReaderWriterLatch latch_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie object. Initialize the root node with '\0'
   * character.
   */
  Trie() {
    this->root_ = std::unique_ptr<TrieNode>(new TrieNode('\0'));
    // LOG_INFO("#Trie:constructor = %c", this->root_->GetKeyChar());
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert key-value pair into the trie.
   *
   * If the key is an empty string, return false immediately.
   *
   * If the key already exists, return false. Duplicated keys are not allowed and
   * you should never overwrite value of an existing key.
   *
   * When you reach the ending character of a key:
   * 1. If TrieNode with this ending character does not exist, create new TrieNodeWithValue
   * and add it to parent node's children_ map.
   * 2. If the terminal node is a TrieNode, then convert it into TrieNodeWithValue by
   * invoking the appropriate constructor.
   * 3. If it is already a TrieNodeWithValue,
   * then insertion fails and returns false. Do not overwrite existing data with new data.
   *
   * You can quickly check whether a TrieNode pointer holds TrieNode or TrieNodeWithValue
   * by checking the is_end_ flag. If is_end_ == false, then it points to TrieNode. If
   * is_end_ == true, it points to TrieNodeWithValue.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param value Value to be inserted
   * @return True if insertion succeeds, false if the key already exists
   */
  template <typename T>
  bool Insert(const std::string &key, T value) {
    if (key.size() == 0) {
      return false;
    }

    TrieNode *cur = this->root_.get();
    for (size_t i = 0; i < key.size(); i++) {
      if (i < key.size() - 1) {
        if (cur->HasChild(key[i]) == false) {
          TrieNode node(key[i]);
          std::unique_ptr<TrieNode> new_node_ptr(new TrieNode(std::move(node)));
          cur->InsertChildNode(key[i], std::move(new_node_ptr));
        }
        cur = cur->GetChildNode(key[i])->get();
      } else {
        //  1. If TrieNode with this ending character does not exist, create new TrieNodeWithValue
        // and add it to parent node's children_ map.
        if (cur->HasChild(key[i]) == false) {
          TrieNodeWithValue<T> node(key[i], value);
          node.SetEndNode(true);
          std::unique_ptr<TrieNodeWithValue<T>> new_node_ptr(new TrieNodeWithValue<T>(std::move(node), value));
          cur->InsertChildNode(key[i], std::move(new_node_ptr));
          break;
        }

        if (cur->HasChild(key[i]) == true) {
          // 2. If the terminal node is a TrieNode, then convert it into TrieNodeWithValue by
          // invoking the appropriate constructor.
          if ((*cur->GetChildNode(key[i]))->IsEndNode() == false) {
            TrieNodeWithValue<T> node(std::move(*(*cur->GetChildNode(key[i]))), value);
            std::unique_ptr<TrieNodeWithValue<T>> new_node_ptr(new TrieNodeWithValue<T>(std::move(node), value));
            cur->InsertChildNode(key[i], std::move(new_node_ptr));
          }
          // 3. If it is already a TrieNodeWithValue,
          // then insertion fails and returns false. Do not overwrite existing data with new data.
          return false;
        }
      }
    }
    return true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove key value pair from the trie.
   * This function should also remove nodes that are no longer part of another
   * key. If key is empty or not found, return false.
   *
   * You should:
   * 1) Find the terminal node for the given key.
   * 2) If this terminal node does not have any children, remove it from its
   * parent's children_ map.
   * 3) Recursively remove nodes that have no children and are not terminal node
   * of another key.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @return True if the key exists and is removed, false otherwise
   */
  bool Remove(const std::string &key) {
    if (key.empty() == true) {
      return false;
    }

    TrieNode *cur = this->root_.get();
    std::vector<TrieNode *> nodes;
    for (size_t i = 0; i < key.size(); i++) {
      nodes.push_back(cur);
      if (!cur->HasChild(key[i])) {
        return false;
      }
      cur = (*cur->GetChildNode(key[i])).get();
    }
    cur->SetEndNode(false);
    while (!nodes.empty()) {
      if (!cur->HasChildren()) {
        TrieNode *parent = nodes.back();
        parent->RemoveChildNode(cur->GetKeyChar());
        nodes.pop_back();
        cur = parent;
      }
      break;
    }
    return true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the corresponding value of type T given its key.
   * If key is empty, set success to false.
   * If key does not exist in trie, set success to false.
   * If the given type T is not the same as the value type stored in TrieNodeWithValue
   * (ie. GetValue<int> is called but terminal node holds std::string),
   * set success to false.
   *
   * To check whether the two types are the same, dynamic_cast
   * the terminal TrieNode to TrieNodeWithValue<T>. If the casted result
   * is not nullptr, then type T is the correct type.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param success Whether GetValue is successful or not
   * @return Value of type T if type matches
   */
  template <typename T>
  T GetValue(const std::string &key, bool *success) {
    TrieNode *cur = this->root_.get();
    for (size_t i = 0; i < key.size(); i++) {
      if (i < key.size() - 1) {
        if (cur->HasChild(key[i]) == false) {
          break;
        }
        cur = cur->GetChildNode(key[i])->get();
      } else {
        if (cur->HasChild(key[i]) == false) {
          break;
        }
        TrieNodeWithValue<T> *valueNode = dynamic_cast<TrieNodeWithValue<T> *>((*cur->GetChildNode(key[i])).get());
        if (cur->HasChild(key[i]) == true && valueNode != nullptr) {
          *success = true;
          return valueNode->GetValue();
        }
      }
    }
    *success = false;
    return {};
  }
};

/**
 * dynamic cast unique_ptr of base class to unqiue_ptr of derived class.
 */
template <typename Derived, typename Base>
std::unique_ptr<Derived> DynamicPtrCast(std::unique_ptr<Base> &&base) noexcept {
  if (auto derived = dynamic_cast<Derived *>(base.get())) {
    base.release();
    return std::unique_ptr<Derived>(derived);
  }
  return nullptr;
}

}  // namespace bustub
