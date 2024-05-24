//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_delete_test.cpp
//
// Identification: test/storage/b_plus_tree_delete_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>
#include <random>
#include <iostream>

#include "buffer/buffer_pool_manager_instance.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

// TEST(BPlusTreeTests, DISABLED_DeleteTest1) {
TEST(BPlusTreeTests, DeleteTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  std::vector<int64_t> remove_keys = {1, 5};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, 3);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

// TEST(BPlusTreeTests, DISABLED_DeleteTest2) {
TEST(BPlusTreeTests, DeleteTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

#define USE_RANDOM_DATA 1
TEST(BPlusTreeTests, RandomTest) {
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());
    BufferPoolManager *bpm = new BufferPoolManagerInstance(50, new DiskManager("test.db"));
    std::random_device random;

#if USE_RANDOM_DATA
    int internal_page_max_size = random() % 5 + 2;
    int leaf_page_max_size = random() % 5 + internal_page_max_size;
#else
    int leaf_page_max_size = 8;
    int internal_page_max_size = 5;
#endif

    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("test", bpm, comparator, internal_page_max_size,
                                                             leaf_page_max_size);
    GenericKey<8> index_key;
    RID rid;

    auto transaction = new Transaction(0);
    page_id_t header_page_id;
    [[maybe_unused]] auto header_page = bpm->NewPage(&header_page_id);

    ASSERT_EQ(header_page_id, HEADER_PAGE_ID);

#if USE_RANDOM_DATA
    int len = random() % 1000;
    std::vector<int64_t> keys(len);
    for (int i = 0; i < len; i++) {
        keys.at(i) = random() % 500;
    }
#else
    std::vector<int64_t> keys = {5, 20, 1, 44, 22, 42, 5, 49, 41, 12, 29, 3, 43, 33, 26, 44, 44, 29, 2, 46};
    std::vector<int64_t> del_keys = {41, 42, 3, 5, 26, 1, 49, 29, 20, 5, 44, 33, 29, 22, 12, 44, 44, 2, 46, 43};
#endif

    std::cout << leaf_page_max_size << " " << internal_page_max_size << std::endl;
    for (size_t i = 0; i < keys.size(); i++) {
        std::cout << keys.at(i) << ((i == keys.size() - 1) ? "\n" : ", ");
    }

    for (auto key : keys) {
        rid.Set(static_cast<int32_t>(key >> 32), key);
        index_key.SetFromInteger(key);

        tree.Insert(index_key, rid, transaction);
        tree.Draw(bpm, "/Users/liaohan/CLionProjects/bustub/cmake-build-debug/test/pic");
        EXPECT_EQ(bpm->GetUnpinCount(), 1);
    }

    std::vector<RID> rids;
    for (auto key : keys) {
        rids.clear();
        index_key.SetFromInteger(key);
        tree.GetValue(index_key, &rids);
        EXPECT_EQ(rids.size(), 1);
        EXPECT_EQ(rids[0].GetSlotNum(), key);
        EXPECT_EQ(bpm->GetUnpinCount(), 1);
    }

#if USE_RANDOM_DATA
    std::shuffle(keys.begin(), keys.end(), random);
#else
    keys = del_keys;
#endif

    for (size_t i = 0; i < keys.size(); i++) {
        std::cout << keys.at(i) << ((i == keys.size() - 1) ? "\n" : ", ");
    }

    for (auto key : keys) {
        index_key.SetFromInteger(key);
        tree.Remove(index_key, transaction);
        if (tree.GetRootPageId() != INVALID_PAGE_ID) {
            tree.Draw(bpm, "dot文件路径");
        }
        EXPECT_EQ(bpm->GetUnpinCount(), 1);
    }

    bpm->UnpinPage(HEADER_PAGE_ID, true);
    EXPECT_EQ(tree.GetRootPageId(), INVALID_PAGE_ID);
    EXPECT_EQ(bpm->GetUnpinCount(), 0);
    delete transaction;
    delete bpm;
    remove("test.db");
    remove("test.log");
}

}


}  // namespace bustub
