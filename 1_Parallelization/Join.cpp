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
#include <omp.h>
#include <thread>
#include <algorithm>
#include <ranges>


std::vector<ResultRelation> performNestedLoopJoin(const std::vector<CastRelation>& castRelation, const std::vector<TitleRelation>& titleRelation, int numThreads) {
    std::vector<ResultRelation> resultTuples;

    for (const auto& castTuple : castRelation) {
        for (const auto& titleTuple : titleRelation) {
            if (castTuple.movieId == titleTuple.titleId) {
                resultTuples.push_back(createResultTuple(castTuple, titleTuple));
            }
        }
    }
    return resultTuples;
}

void processChunk(const std::vector<CastRelation>::iterator start, const std::vector<CastRelation>::iterator end,
                  std::vector<TitleRelation>& rightRelation, std::vector<ResultRelation>& results, std::mutex& m_results) {
    std::cout << std::this_thread::get_id() << ": Started processing chunk\n";
    std::forward_iterator auto r_it = std::ranges::lower_bound(
            rightRelation.begin(), rightRelation.end(), TitleRelation{.imdbId = start->movieId},
            [](const TitleRelation& a, const TitleRelation& b) {
                return a.imdbId < b.imdbId;
            }
    );
    if(r_it == rightRelation.end()) {
        std::cout << std::this_thread::get_id() << ": Chunk started with a movieId larger than all TitleRelations.imdbId\n";
    }
    auto l_it = start;
    while(l_it != end && r_it != rightRelation.end()) {
        if(l_it->movieId < r_it->imdbId) {
            ++l_it;
        } else if (l_it->movieId > r_it->imdbId) {
            ++r_it;
        } else {
            std::scoped_lock l_results(m_results);
            results.emplace_back(createResultTuple(*l_it, *r_it));
            ++l_it;
            ++r_it;
        }
    }
    std::cout << std::this_thread::get_id() << ": has finished it's chunk\n";
}

std::vector<ResultRelation> performJoin(const std::vector<CastRelation>& leftRelationConst, const std::vector<TitleRelation>& rightRelationConst,
                                        int numThreads = std::jthread::hardware_concurrency()) {
    std::vector<CastRelation> leftRelation(std::move(leftRelationConst));
    std::vector<TitleRelation> rightRelation(std::move(rightRelationConst));

    std::vector<ResultRelation> results;
    std::mutex m_results;

    std::jthread t1(sortCastRelation, leftRelation.begin(), leftRelation.end());
    std::jthread t2(sortTitleRelation, rightRelation.begin(), rightRelation.end());

    t1.join();
    t2.join();

    size_t chunkSize = leftRelation.size() / numThreads;
    if(chunkSize == 0) {
        // numThreads is larger than data size
        return performNestedLoopJoin(leftRelationConst, rightRelationConst, std::jthread::hardware_concurrency());
    }
    std::vector<std::jthread> threads;
    auto chunkStart = leftRelation.begin();
    for(int i = 0; i < numThreads; ++i) {
        std::vector<CastRelation>::iterator chunkEnd;
        if(i == (numThreads - 1)) {
            chunkEnd = leftRelation.end();
        } else {
            chunkEnd = std::next(chunkStart, chunkSize);
        }
        threads.push_back(
                std::jthread(processChunk, chunkStart, chunkEnd, std::ref(rightRelation), std::ref(results), std::ref(m_results))
        );
        chunkStart = chunkEnd;
    }
    /*
    for(auto& t: threads) {
        t.join();
    }
    */
    return results;
}

TEST(ParallelizationTest, TestJoiningTuples) {
    std::cout << "Test reading data from a file.\n";
    auto leftRelation = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_uniform.csv"), 10000);
    auto rightRelation = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform.csv"), 10000);

    Timer timer("Parallelized Join execute");
    timer.start();

    auto resultTuples = performJoin(leftRelation, rightRelation, 8);

    timer.pause();

    std::cout << "Timer: " << timer << std::endl;
    std::cout << "Result size: " << resultTuples.size() << std::endl;
    std::cout << "\n\n";
}
