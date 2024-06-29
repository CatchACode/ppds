//
// Created by klaas on 28.06.24.
//

#include <gtest/gtest.h>
#include <thread>

#include "Trie.h"
#include "JoinUtils.hpp"

class TestTrie : public ::testing::Test {
protected:
    const std::vector<CastRelation> castTuples = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_uniform1mb.csv"));;
    void SetUp() override {
        // Code here will be called immediately after the constructor (right before each test).
    }

    void TearDown() override {
        // Code here will be called immediately after each test (right before the destructor).
    }

};

TEST_F(TestTrie, TestSingleThreadedInsertion) {
    Trie trie;
    for(const auto& castTuple : castTuples) {
        trie.insert(std::string_view(castTuple.note, 100), &castTuple);
    }
}

TEST_F(TestTrie, TestingThreadedInsertion) {
    Trie trie;
    std::atomic_size_t counter = 0;
    std::vector<std::jthread> threads;
    threads.reserve(std::jthread::hardware_concurrency());
    for(int i = 0; i < std::jthread::hardware_concurrency(); ++i) {
        threads.emplace_back(
                [&trie, &counter, i, this] {
                    while(counter < castTuples.size()) {
                        trie.insert(std::string_view(castTuples[counter++].note, 100), &castTuples[counter]);
                    }
                }
                );
    }
}