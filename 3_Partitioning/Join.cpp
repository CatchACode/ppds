/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

//#include "TimerUtil.hpp"
#include "JoinUtils.hpp"
#include <unordered_map>
#include <iostream>
#include <gtest/gtest.h>
#include "Partitioning.h"
#include <bitset>
#include "HashJoin.h"
#include "TimerUtil.hpp"

std::vector<ResultRelation> performJoin(const std::vector<CastRelation>& castRelation, const std::vector<TitleRelation>& titleRelation, int numThreads) {
    auto results = performPartitionJoin(castRelation, titleRelation, numThreads);
    //auto results = performCacheSizedThreadedHashJoin(castRelation, titleRelation);
    std::cout << "castRelation.size(): " << castRelation.size() << '\n';
    std::cout << "titleRelation.size(): " << titleRelation.size() << '\n';
    std::cout << "results.size(): " << results.size() << '\n';
    return results;
}

TEST(PartioningTest, TestJoiningTuples) {
    const auto leftRelation = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_uniform.csv"));
    const auto rightRelation = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform.csv"));

    auto results = performJoin(leftRelation, rightRelation, 8);
}


TEST(PartioningTest, TestGetBitAtPosition) {
    uint64_t num = 0b1111;
    for(int i = 0; i < 4; ++i) {
        assert(getBitAtPosition(num, i) == true);
    }
}

TEST(PartioningTest, TestAppendStep) {
    uint8_t steps = 0;
    assert(appendStep(steps, 0, 0)==0);
    assert(appendStep(steps, 1, 0)==1);
    assert(appendStep(steps, 1, 1)==2);
    assert(appendStep(steps, 0, 1)==0);
    steps = 1;
    assert(appendStep(steps,1,1)==3);
}


TEST(PartioningTest, TestCastRadixPartition) {
    std::vector<uint32_t> castRelation {0b000, 0b001, 0b010, 0b100, 0b011, 0b101, 0b110, 0b111};
    for(const auto& num: castRelation) {
        std::bitset<sizeof(uint32_t)> temp(num);
        std::cout << temp << std::endl;
    }
}

TEST(PartioningTest, castPartition) {
    auto leftRelation = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_uniform1gb.csv"));
    setMaxBitsToCompare(leftRelation.size());
    ThreadPool threadPool(16);
    std::mutex m;
    std::atomic_size_t counter(0);
    std::vector<std::span<CastRelation>> results;
    results.resize(numPartitionsToExpect);
    //threadPool.enqueue(castPartition, std::ref(threadPool), leftRelation.begin(), leftRelation.end(), 0, std::ref(m), std::ref(results), std::ref(counter));
    size_t expected = numPartitionsToExpect;
    while(size_t current = counter.load() < expected) {
        counter.wait(current);
    }
    std::cout << "counter: " << counter.load() << std::endl;
    for(const auto& span: results) {
        for(const auto& record: span) {
            std::cout << std::bitset<sizeof(int32_t)*8>(record.movieId) << std::endl;
        }
    }
}

TEST(PartioningTest, titlePartition) {
    auto rightRelation = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform1gb.csv"));
    setMaxBitsToCompare(rightRelation.size());
    ThreadPool threadPool(16);
    std::mutex m;
    std::atomic_size_t counter(0);
    std::vector<std::span<TitleRelation>> results;
    results.resize(numPartitionsToExpect);
    //threadPool.enqueue(titlePartition, std::ref(threadPool), rightRelation.begin(), rightRelation.end(), 0, std::ref(m), std::ref(results), std::ref(counter));
    size_t expected = numPartitionsToExpect;
    while(size_t current = counter.load() < expected) {
        counter.wait(current);
    }
    std::cout << "counter: " << counter.load() << std::endl;
    /*
    for(const auto& span: results) {
        for(const auto& record: span) {
            std::cout << std::bitset<sizeof(int32_t)*8>(record.titleId) << std::endl;
        }
    }
    */
}


TEST(PartitioningTest, TestTitleRadixPartition) {
    auto titleRelations = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform1mb.csv"));

    auto split = titleRadixPartition(titleRelations.begin(), titleRelations.end(), 0);
    for(auto it = titleRelations.begin(); it != split; ++it) {
        std::cout << std::bitset<sizeof(int32_t)*8>(it->titleId) << std::endl;
    }
    for(auto it = split; it != titleRelations.end(); ++it) {
        std::cout << std::bitset<sizeof(int32_t)*8>(it->titleId) << std::endl;
    }
}

TEST(PartitioningTest, TestCastRadixPartition) {
    auto castRelations = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_uniform1mb.csv"));
    auto split = castRadixPartition(castRelations.begin(), castRelations.end(), 0);
    for(auto it = castRelations.begin(); it != split; ++it) {
        std::cout << std::bitset<sizeof(int32_t)*8>(it->movieId) << std::endl;
    }
    for(auto it = split; it != castRelations.end(); ++it) {
        std::cout << std::bitset<sizeof(int32_t)*8>(it->movieId) << std::endl;
    }
}

TEST(PartitioningTest, TestBitMask) {
    maxBitsToCompare = 3;
    assert(bitmask() == 0b111);
}


TEST(PartitioningTest, TestMatchingBins) {
    auto castRelations = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_uniform1mb.csv"));
    auto titleRelations = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform.csv"));
    std::vector<std::span<CastRelation>> castPartitions;
    std::vector<std::span<TitleRelation>> titlePartitions;
    ThreadPool threadPool(std::thread::hardware_concurrency());
    //partition(threadPool, castRelations, titleRelations, castPartitions, titlePartitions);
    std::cout << "castPartitions: " << castPartitions.size() << '\n';
    std::cout << "titleParitions: " << titlePartitions.size() << '\n';

    for(int i = 0; i < 16; ++i) {
        std::cout << "Comparing partition " << i << '\n';
        std::cout << "titlepartion[" << i << "] contains:\n";
        for(const auto& record: titlePartitions[i]) {
            std::cout << record.titleId << '\n';
        }
        std::cout << "castPartition[" << i << "] contains:\n";
        for(const auto& record: castPartitions[i]) {
            std::cout << record.movieId << '\n';
        }
        std::cout << '\n';
    }
}


TEST(PartitioningTest, TestPerformJoin) {
    const auto castRelations = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_zipfian1gb.csv"));
    const auto titleRelations = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform1gb.csv"));

    Timer timer("timer");
    timer.start();
    auto results = performPartitionJoin(castRelations, titleRelations, 16);
    timer.pause();
    std::cout << "Join Time: " << printString(timer) << '\n';
    //auto results = performCacheSizedThreadedHashJoin(castRelations, titleRelations, std::jthread::hardware_concurrency());
    std::cout << "results.size(): " << results.size() << '\n';
}

TEST(PartitioningTest, TestBullshit) {
    Timer timer("t");
    std::vector<ResultRelation> results;
    timer.start();
    results.reserve(26810);
    timer.pause();
    std::cout << "Time: " << printString(timer) << '\n';
}