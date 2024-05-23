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
#include "ThreadedLoad.h"
#include "HashJoin.h"
#include "SortMergeJoin.h"
#include "NestedLoopJoin.h"
#include "MergeSort.h"

#include <unordered_map>
#include <iostream>
#include <gtest/gtest.h>
#include <omp.h>
#include <thread>
#include <algorithm>
#include <ranges>
#include <span>
//#include <experimental/simd>



std::vector<ResultRelation> performJoin(const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation, int numThreads = std::jthread::hardware_concurrency()) {
    return performSHJ_MAP(leftRelation, rightRelation);
}






TEST(ParallelizationTest, TestJoiningTuples) {
    const auto leftRelation = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_uniform.csv"));
    const auto rightRelation = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform.csv"));

    std::cout << "Sizeof leftRelation: " << leftRelation.size()*sizeof(CastRelation) / (1024*1024) << '\n';
    std::cout << "Sizeof rightReleation: " << rightRelation.size()*sizeof(TitleRelation) / (1024*1024) << '\n';


    Timer timer("Parallelized Join execute");
    timer.start();

    auto resultTuples = performThreadedSortJoin(leftRelation, rightRelation);

    timer.pause();

    std::cout << "Timer: " << timer << std::endl;
    std::cout << "Result size: " << resultTuples.size() << std::endl;
    std::cout << "\n\n";
}

TEST(ParallelizationTest, TestThreadScaling) {
    const auto leftRelation = threadedLoad<CastRelation>(DATA_DIRECTORY + std::string("cast_info_uniform_512mb.csv"));
    const auto rightRelation = threadedLoad<TitleRelation>(DATA_DIRECTORY + std::string("title_info_uniform_512mb.csv"));
    auto sink = performThreadedSortJoin(leftRelation, rightRelation); // So Cache is hot?

    for(int i = 1; i <= std::thread::hardware_concurrency(); ++i) {
        Timer timer("Parallelized Join Execute");
        timer.start();
        auto resultTuples = performThreadedSortJoin(leftRelation, rightRelation, i);
        timer.pause();
        std::cout << "Timer for numThreads=" << i <<": " << timer << std::endl;
        std::cout << "\n\n";
    }
}

TEST(ParallelizationTest, TestIdenticalKeys) {
    auto leftRelation = load<CastRelation>(DATA_DIRECTORY + std::string("cast_info_all_join_keys_equal.csv"));
    auto rightRelation = load<TitleRelation>(DATA_DIRECTORY + std::string("title_info_all_join_keys_equal.csv"));

    std::cout << "Sizeof leftRelation: " << sizeof(leftRelation) << '\n';
    std::cout << "Sizeof rightReleation: " << sizeof(rightRelation) << '\n';

    sortCastRelation(leftRelation.begin(), leftRelation.end());
    sortTitleRelation(rightRelation.begin(), rightRelation.end());

    auto results = performHashJoin(SHJ_UNORDERED_MAP, leftRelation, rightRelation);

    for(const auto& record: results) {
        std::cout << resultRelationToString(record) << '\n';
    }
    std::cout << "\n\n";
}

TEST(ParalleizationTest, TestOMPMergeSort) {
    auto leftRelation = threadedLoad<CastRelation>(DATA_DIRECTORY + std::string("cast_info_uniform.csv"));
    auto rightRelation = threadedLoad<TitleRelation>(DATA_DIRECTORY + std::string("title_info_uniform.csv"));

    Timer timer("OMP MergeSort");
    timer.start();
    timer.pause();

    std::cout << timer << std::endl;
}

TEST(ParalleizationTest, TestCheapParallelSort) {
    auto castRelation = threadedLoad<CastRelation>(DATA_DIRECTORY + std::string("cast_info_uniform.csv"));

    cheapParallelSort<CastRelation>(castRelation, compareCastRelations, 8);
    assert(std::is_sorted(castRelation.begin(), castRelation.end(), compareCastRelations));
}

TEST(ParalleizationTest, TestMergeSortGPT) {
    auto castRelation = threadedLoad<CastRelation>(DATA_DIRECTORY + std::string("cast_info_uniform.csv"));
    ThreadPool threadPool(std::jthread::hardware_concurrency());
    merge_sort(threadPool, castRelation.begin(), castRelation.end(), compareCastRelations, std::jthread::hardware_concurrency());
    assert(std::is_sorted(castRelation.begin(), castRelation.end(), compareCastRelations));
}