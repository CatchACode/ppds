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

#include "TimerUtil.hpp"
#include "JoinUtils.hpp"
#include <unordered_map>
#include <iostream>
#include <gtest/gtest.h>
#include "Partitioning.h"
#include <bitset>

std::vector<ResultRelation> performJoin(const std::vector<CastRelation>& castRelation, const std::vector<TitleRelation>& titleRelation, int numThreads) {

    // TODO: Implement your join
    // The benchmark will test it against skewed key distributions
    //return performCacheSizedThreadedHashJoin(castRelation, titleRelation, numThreads);
    return {};
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

TEST(PartioningTest, uint32Partiton) {
    std::vector<int32_t> castRelation {0b000, 0b001, 0b010, 0b100, 0b011, 0b101, 0b110, 0b111, 0b1000, 0b1001, 0b1010,0b1100,0b1101,0b1111};
    auto threadPool = std::make_shared<ThreadPool>(1);

    uint32Partition(castRelation);
    for(const auto& num: castRelation) {
        std::bitset<sizeof(uint32_t)> temp(num);
        std::cout << temp << std::endl;
    }

}

TEST(PartioningTest, partitioning) {
    auto leftRelation = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_uniform.csv"));
    auto rightRelation = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform.csv"));
    partition(std::span(leftRelation), std::span(rightRelation), 8);
    for(const auto& record: leftRelation) {
        std::cout << std::bitset<sizeof(int32_t)>(record.movieId) << std::endl;
    }
}

TEST(PartioningTest, castPartition) {
    auto leftRelation = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_uniform.csv"));
    auto threadPool =   std::make_shared<ThreadPool>(8);
    std::mutex m_castPartitions;
    std::vector<std::span<CastRelation>> castPartitions;
    std::atomic_size_t counter(0);

    castPartition(threadPool, std::span(leftRelation), 0, m_castPartitions, castPartitions, counter);
    sleep(1);
}

TEST(PartioningTest, titlePartition) {
    auto rightRelation = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform.csv"));
    auto threadPool = std::make_shared<ThreadPool>(1);
    std::mutex m;
    std::vector<std::span<TitleRelation>> titlePartitions;
    titlePartitions.reserve(1000);
    std::atomic_size_t counter(0);
}


TEST(PartitioningTest, TestTitleRadixPartition) {
    auto titleRelations = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform.csv"));

    auto split = titleRadixPartition(titleRelations.begin(), titleRelations.end(), 0);
    for(auto it = titleRelations.begin(); it != split; ++it) {
        std::cout << std::bitset<sizeof(int32_t)>(it->titleId) << std::endl;
    }
    for(auto it = split; it != titleRelations.end(); ++it) {
        std::cout << std::bitset<sizeof(int32_t)>(it->titleId) << std::endl;
    }
}