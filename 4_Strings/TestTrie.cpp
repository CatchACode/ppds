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
    const std::vector<CastRelation> stolenCastTuples = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_stolen_strings.csv"), 100);;
    const std::vector<TitleRelation> thirtyTitleTuples = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_short_strings_30.csv"));;
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
        const auto result = trie.search(std::string_view(castTuple.note, 100));
        ASSERT_EQ(result[0], &castTuple);
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
                        const auto result = trie.search(std::string_view(castTuples[localCounter].note, 100));
                        EXPECT_EQ(result[0], &castTuples[localCounter]);
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
        const auto result = trie.search(std::string_view(castTuple.note, 100));
        int test = std::memcmp(result[0], &castTuple, sizeof(CastRelation));
        std::cout << "Testing id: " << castTuple.castInfoId << " with result: " << test << "\n";
        EXPECT_EQ(test, 0) << "Failed comparsion with notes:\n" << castTuple.note << "\n" << result[0]->note << "\n";
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

    auto results = trie.search("apple");
    EXPECT_EQ(results[0], &a);
    results = trie.search("app");
    EXPECT_EQ(results[0], &b);
}

TEST_F(TestTrie, TestPrefixes) {
    std::string dnbme = "Don't Be a Menace to South Central While Drinking Your Juice in the Hood (1996)";
    std::string s = "Don't";
    std::string s2 = "Don't";

    Trie<uint32_t> trie;
    uint32_t a = 1;
    uint32_t b = 2;
    trie.insert(s, &a);
    trie.insert(s2, &a);
    trie.insert(s2, &b);
    auto resultPtr = trie.longestPrefix(dnbme);
    EXPECT_EQ(resultPtr[0], &a);
    EXPECT_EQ(resultPtr[1], &a);
    EXPECT_EQ(resultPtr[2], &b);
}

TEST_F(TestTrie, TestStolenDataCastInsertion) {
    Trie<CastRelation> trie;
    std::atomic_size_t counter = 0;
    std::vector<std::jthread> threads;
    threads.reserve(std::jthread::hardware_concurrency());
    Timer timer("Trie");
    timer.start();
    for(int i = 0; i < std::jthread::hardware_concurrency(); ++i) {
        threads.emplace_back(
                [&trie, &counter, i, this] {
                    while(counter < stolenCastTuples.size()) {
                        auto localCounter = counter.fetch_add(1);
                        trie.insert(std::string_view(stolenCastTuples[localCounter].note, 100), &stolenCastTuples[localCounter]);
                    }
                }
        );
    }
    for(auto& thread : threads) {
        thread.join();
    }
    timer.pause();
    std::cout << "Insertion took: " << printString(timer) << "ms" << std::endl;
    // Validate the insertion
    for(const auto& castTuple : stolenCastTuples) {
        const auto result = trie.search(std::string_view(castTuple.note, 100));
        int test = std::memcmp(result[0], &castTuple, sizeof(CastRelation));
        std::cout << "Testing id: " << castTuple.castInfoId << " with result: " << test << "\n";
        EXPECT_EQ(test, 0) << "Failed comparsion with notes:\n" << castTuple.note << "\n" << result[0]->note << "\n";
    }
}

TEST_F(TestTrie, TestMultipleSharedNodes) {
    const auto castTuples = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_broken_values.csv"));
    Trie<CastRelation> trie;
    for(const auto& castTuple : castTuples) {
        trie.insert(std::string_view(castTuple.note, 100), &castTuple);
    }
    for(const auto& castTuple : castTuples) {
        const auto result = trie.search(std::string_view(castTuple.note, 100));
        bool found = false;
        for(const auto& res : result) {
            // Check if res and castTuple point to the same memory location
            if(res == &castTuple) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found) << "Failed to find castTuple with id: " << castTuple.castInfoId << " " << castTuple.note;
    }
}