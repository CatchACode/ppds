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

#include <unordered_map>
#include <iostream>
#include <gtest/gtest.h>
#include <omp.h>
#include <thread>
#include <algorithm>
#include <ranges>
#include <span>


/** performs a sorted join, <b>has undefined behaviour if the spans are not sorted!</b>
 *
 * @param castRelation a sorted span of cast records
 * @param titleRelation a sorted span of title records
 * @return a std::vector<ResultRelation> of joined tuples
 */
std::vector<ResultRelation> performSortedJoin(const std::span<CastRelation>& castRelation, const std::span<TitleRelation>& titleRelation) {
    std::vector<ResultRelation> results;

    std::forward_iterator auto l_it = castRelation.begin();
    std::forward_iterator auto r_it = titleRelation.begin();
    while(l_it != castRelation.end() && r_it != titleRelation.end()) {
        if(l_it->movieId < r_it->imdbId) {
            ++l_it;
        } else if (l_it->movieId > r_it->imdbId) {
            ++r_it;
        } else {
            results.emplace_back(createResultTuple(*l_it, *r_it));
            ++l_it;
            ++r_it;
        }
    }
    return results;
}

void inline printResults(const std::vector<ResultRelation>& results) {
    for(auto const& record: results) {
        std::cout << resultRelationToString(record) << '\n';
    }
    std::cout << std::flush;
}

std::vector<ResultRelation> performNestedLoopJoin(const std::vector<CastRelation>& castRelation, const std::vector<TitleRelation>& titleRelation, int numThreads) {
    std::vector<ResultRelation> resultTuples;

    for (const auto& castTuple : castRelation) {
        for (const auto& titleTuple : titleRelation) {
            if (castTuple.movieId == titleTuple.titleId) {
                resultTuples.push_back(createResultTuple(castTuple, titleTuple));
            }
        }
    }
    //printResults(resultTuples);
    return resultTuples;
}



void processChunk(const std::span<CastRelation> castRelation, std::span<TitleRelation> rightRelation,
                  std::vector<ResultRelation>& results, std::mutex& m_results) {
    //std::cout << std::this_thread::get_id() << ": Started processing chunk\n";
    std::forward_iterator auto r_it = std::ranges::lower_bound(
            rightRelation.begin(), rightRelation.end(), TitleRelation{.titleId = castRelation.begin()->movieId},
            [](const TitleRelation& a, const TitleRelation& b) {
                return a.titleId < b.titleId;
            }
    );
    if(r_it == rightRelation.end()) {
        //std::cout << std::this_thread::get_id() << ": Chunk started with a movieId larger than all TitleRelations.imdbId\n";
    }
    auto l_it = castRelation.begin();
    while(l_it != castRelation.end() && r_it != rightRelation.end()) {
        if(l_it->movieId < r_it->titleId) {
            ++l_it;
        } else if (l_it->movieId > r_it->titleId) {
            ++r_it;
        } else {
            std::scoped_lock l_results(m_results);
            results.emplace_back(createResultTuple(*l_it, *r_it));
            ++l_it;
            ++r_it;
        }
    }
    //std::cout << std::this_thread::get_id() << ": has finished it's chunk\n";
}

std::vector<ResultRelation> performJoin(const std::vector<CastRelation>& leftRelationConst, const std::vector<TitleRelation>& rightRelationConst,
                                        int numThreads = std::jthread::hardware_concurrency()) {
    // Putting this here allows for early return on very small data sets
    size_t chunkSize = leftRelationConst.size() / numThreads;
    //std::cout << "chunkSize is: " << chunkSize << '\n';
    if(chunkSize == 0) {
        // numThreads is larger than data size
        return performNestedLoopJoin(leftRelationConst, rightRelationConst, std::jthread::hardware_concurrency());
    }

    std::vector<CastRelation> leftRelation(leftRelationConst);
    std::vector<TitleRelation> rightRelation(rightRelationConst);

    std::vector<ResultRelation> results;
    std::mutex m_results;
    if(numThreads < 2) {
        sortCastRelation(leftRelation.begin(), leftRelation.end());
        sortTitleRelation(rightRelation.begin(), rightRelation.end());
        //std::cout << "Relations sorted!\n";
        return performSortedJoin(std::span(leftRelation), std::span(rightRelation));
    } else {
        std::jthread t1(sortCastRelation, leftRelation.begin(), leftRelation.end());
        std::jthread t2(sortTitleRelation, rightRelation.begin(), rightRelation.end());
        t1.join();
        t2.join();
    }
    //std::cout << "Relations sorted!\n";

    std::vector<std::jthread> threads;
    auto chunkStart = leftRelation.begin();
    for(int i = 0; i < numThreads; ++i) {
        std::vector<CastRelation>::iterator chunkEnd;
        if(i == (numThreads - 1)) {
            chunkEnd = leftRelation.end();
        } else {
            chunkEnd = std::next(chunkStart, chunkSize);
        }
        std::span<CastRelation> chunkSpan(std::to_address(chunkStart), std::to_address(chunkEnd));
        threads.push_back(
                std::jthread(processChunk, chunkSpan, std::span(rightRelation), std::ref(results), std::ref(m_results))
        );
        chunkStart = chunkEnd;
    }
    for(auto& t: threads) {
        t.join();
    }
    //printResults(results);
    return results;
}

TEST(ParallelizationTest, TestJoiningTuples) {
    auto leftRelation = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_uniform.csv"));
    auto rightRelation = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform.csv"));

    Timer timer("Parallelized Join execute");
    timer.start();

    auto resultTuples = performJoin(leftRelation, rightRelation);

    timer.pause();

    std::cout << "Timer: " << timer << std::endl;
    std::cout << "Result size: " << resultTuples.size() << std::endl;
    std::cout << "\n\n";
}

TEST(ParallelizationTest, TestThreadScaling) {
    auto leftRelation = threadedLoad<CastRelation>(DATA_DIRECTORY + std::string("cast_info_uniform.csv"));
    auto rightRelation = threadedLoad<TitleRelation>(DATA_DIRECTORY + std::string("title_info_uniform.csv"));
    performJoin(leftRelation, rightRelation); // So Cache is hot?

    for(int i = 1; i <= std::thread::hardware_concurrency(); ++i) {
        Timer timer("Parallelized Join Execute");
        timer.start();
        auto resultTuples = performJoin(leftRelation, rightRelation, i);
        timer.pause();
        std::cout << "Timer for numThreads=" << i <<": " << timer << std::endl;
        std::cout << "\n\n";
    }
}
