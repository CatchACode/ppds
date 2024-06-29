//
// Created by klaas on 28.06.24.
//

#include <gtest/gtest.h>
#include <thread>

#include "Trie.h"
#include "JoinUtils.hpp"
#include "TimerUtil.hpp"

class TestTrie : public ::testing::Test {
protected:
    const std::vector<CastRelation> castTuples = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_short_strings_20000.csv"));;
    const std::vector<TitleRelation> titleTuples = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_short_strings_20000.csv"));;
    void SetUp() override {
        // Code here will be called immediately after the constructor (right before each test).
    }

    void TearDown() override {
        // Code here will be called immediately after each test (right before the destructor).
    }

};

TEST_F(TestTrie, TestSingleThreadedInsertion) {
    Trie<CastRelation> trie;
    Timer timer("Trie");
    timer.start();
    for(const auto& castTuple : castTuples) {
        trie.insert(std::string_view(castTuple.note, 100), &castTuple);
    }
    timer.pause();
    std::cout << "Insertion took: " << printString(timer) << "ms" << std::endl;
}

TEST_F(TestTrie, TestingThreadedInsertion) {
    Trie<CastRelation> trie;
    std::atomic_size_t counter = 0;
    std::vector<std::jthread> threads;
    threads.reserve(std::jthread::hardware_concurrency());
    Timer timer("Trie");
    timer.start();
    for(int i = 0; i < std::jthread::hardware_concurrency(); ++i) {
        threads.emplace_back(
                [&trie, &counter, i, this] {
                    auto localCounter = counter.fetch_add(1);
                    trie.insert(std::string_view(castTuples[localCounter].note, 100), &castTuples[localCounter]);
                }
                );
    }
    for(auto& thread : threads) {
        thread.join();
    }
    timer.pause();
    std::cout << "Insertion took: " << printString(timer) << "ms" << std::endl;
}

TEST_F(TestTrie, TestSingleThreadedSearch) {
    Trie<CastRelation> trie;
    for(const auto& castTuple : castTuples) {
        trie.insert(std::string_view(castTuple.note, 100), &castTuple);
    }

    for(const auto& castTuple : castTuples) {
        const auto* result = trie.search(std::string_view(castTuple.note, 100));
        ASSERT_EQ(result, &castTuple);
    }
}

TEST_F(TestTrie, TestThreadedSearch) {
    Trie<CastRelation> trie;
    for(const auto& castTuple : castTuples) {
        trie.insert(std::string_view(castTuple.note, 100), &castTuple);
    }

    std::atomic_size_t counter = 0;
    std::vector<std::jthread> threads;
    threads.reserve(std::jthread::hardware_concurrency());
    for(int i = 0; i < std::jthread::hardware_concurrency(); ++i) {
        threads.emplace_back(
                [&trie, &counter, i, this] {
                    while(counter < castTuples.size()) {
                        auto localCounter = counter.fetch_add(1);
                        const auto* result = trie.search(std::string_view(castTuples[localCounter].note, 100));
                        ASSERT_EQ(result, &castTuples[localCounter]);
                    }
                }
                );
    }
}


TEST_F(TestTrie, TestThreadedInsertionValidation) {
    Trie<CastRelation> trie;
    std::atomic_size_t counter = 0;
    std::vector<std::jthread> threads;
    threads.reserve(std::jthread::hardware_concurrency());
    Timer timer("Trie");
    timer.start();
    for(int i = 0; i < std::jthread::hardware_concurrency(); ++i) {
        threads.emplace_back(
                [&trie, &counter, i, this] {
                    while(counter < castTuples.size()) {
                        auto localCounter = counter.fetch_add(1);
                        trie.insert(std::string_view(castTuples[localCounter].note, 100), &castTuples[localCounter]);
                    }
                }
        );
    }
    for(auto& thread : threads) {
        thread.join();
    }
    timer.pause();
    std::cout << "Insertion took: " << printString(timer) << "ms" << std::endl;
    for(const auto& castTuple : castTuples) {
        const auto* result = trie.search(std::string_view(castTuple.note, 100));
        int test = std::memcmp(result, &castTuple, sizeof(CastRelation));
        std::cout << "Testing id: " << castTuple.castInfoId << " with result: " << test << "\n";
        EXPECT_EQ(test, 0) << "Failed comparsion with notes:\n" << castTuple.note << "\n" << result->note << "\n";
    }
}

TEST_F(TestTrie, TestJoining) {
    Trie<CastRelation> trie;
    std::atomic_size_t counter = 0;
    std::vector<std::jthread> threads;
    threads.reserve(std::jthread::hardware_concurrency());
    Timer timer("Trie");
    timer.start();
    for(int i = 0; i < std::jthread::hardware_concurrency(); ++i) {
        threads.emplace_back(
                [&trie, &counter, i, this] {
                    while(counter < castTuples.size()) {
                        auto localCounter = counter.fetch_add(1);
                        trie.insert(std::string_view(castTuples[localCounter].note, 100), &castTuples[localCounter]);
                    }
                }
        );
    }
    for(auto& thread : threads) {
        thread.join();
    }
    timer.pause();
    std::cout << "Insertion took: " << printString(timer) << "ms" << std::endl;
}


TEST_F(TestTrie, TestSharingNodes) {
    Trie<uint32_t> trie;
    uint32_t a = 1;
    uint32_t b = 2;
    trie.insert("apple", &a);
    trie.insert("app", &b);

    const uint32_t* result = trie.search("apple");
    EXPECT_EQ(result, &a);
    result = trie.search("app");
    EXPECT_EQ(result, &b);

}