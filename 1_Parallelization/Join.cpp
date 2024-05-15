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
#include <experimental/simd>


/** performs a sorted join, <b>has undefined behaviour if the spans are not sorted!</b>
 *
 * @param castRelation a sorted span of cast records
 * @param titleRelation a sorted span of title records
 * @return a std::vector<ResultRelation> of joined tuples
 */
std::vector<ResultRelation> performSortedJoin(const std::span<CastRelation>& castRelation, const std::span<TitleRelation>& titleRelation) {
    std::vector<ResultRelation> results;

    int32_t currentId = 0;
    std::forward_iterator auto l_it = castRelation.begin();
    std::forward_iterator auto r_it = titleRelation.begin();
    while(l_it != castRelation.end() && r_it != titleRelation.end()) {
        if(l_it->movieId < r_it->titleId) {
            ++l_it;
        } else if (l_it->movieId > r_it->titleId) {
            ++r_it;
        } else {
            std::forward_iterator auto r_start = r_it;
            std::forward_iterator auto l_start = l_it;
            currentId = r_it->titleId;

            // Find End of block where both sides share keys
            while(r_it != titleRelation.end() && r_it->titleId == currentId) {
                ++r_it;
            }
            while(l_it != castRelation.end() && l_it->movieId == currentId) {
                ++l_it;
            }

            for(std::forward_iterator auto l_idx = l_start; l_idx != l_it; ++l_idx) {
                for(std::forward_iterator auto r_idx = r_start; r_idx != r_it; ++r_idx) {
                    results.emplace_back(createResultTuple(*l_idx, *r_idx));
                }
            }

        }
    }
    return results;
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
    return resultTuples;
}



void processChunk(const std::span<CastRelation> castRelation, std::span<TitleRelation> rightRelation,
                  std::vector<ResultRelation>& results, std::mutex& m_results) {
    std::cout << std::this_thread::get_id() << ": Started processing chunk\n";
    std::forward_iterator auto r_it = std::ranges::lower_bound(
            rightRelation.begin(), rightRelation.end(), TitleRelation{.titleId = castRelation.begin()->movieId},
            [](const TitleRelation& a, const TitleRelation& b) {
                return a.titleId < b.titleId;
            }
    );
    if(r_it == rightRelation.end()) {
        std::cout << std::this_thread::get_id() << ": Chunk started with a movieId larger than all TitleRelations.imdbId\n"
        << r_it->titleId << '>' << castRelation[0].movieId << '\n';
    }
    auto l_it = castRelation.begin();
    int32_t currentId = 0;
    while(l_it != castRelation.end() && r_it != rightRelation.end()) {
        if(l_it->movieId < r_it->titleId) {
            ++l_it;
        } else if (l_it->movieId > r_it->titleId) {
            ++r_it;
        } else {
            std::forward_iterator auto r_start = r_it;
            std::forward_iterator auto l_start = l_it;
            currentId = r_it->titleId;

            // Find End of block where both sides share keys
            while(r_it != rightRelation.end() && r_it->titleId == currentId) {
                ++r_it;
            }
            while(l_it != castRelation.end() && l_it->movieId == currentId) {
                ++l_it;
            }

            for(std::forward_iterator auto l_idx = l_start; l_idx != l_it; ++l_idx) {
                for(std::forward_iterator auto r_idx = r_start; r_idx != r_it; ++r_idx) {
                    results.push_back(createResultTuple(*l_idx, *r_idx));
                }
            }

        }
    }
    std::cout << std::this_thread::get_id() << ": has finished it's chunk\n";
}

void mergeVectors(std::vector<std::vector<ResultRelation>>& resultVectors, std::vector<ResultRelation>& results) {
    for(const auto& vector: resultVectors) {
        results.reserve(results.size() + vector.size());
        std::move(vector.begin(), vector.end(), std::back_inserter(results));
    }
}

std::vector<ResultRelation> performThreadedSortJoin(const std::vector<CastRelation>& leftRelationConst, const std::vector<TitleRelation>& rightRelationConst,
                                        int numThreads = std::jthread::hardware_concurrency()) {
    // Putting this here allows for early return on very small data sets
    size_t chunkSize = leftRelationConst.size() / numThreads;
    std::cout << "chunkSize is: " << chunkSize << '\n';
    std::cout << "numThreads is: " << numThreads << '\n';
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
        std::cout << "Relations sorted!\n";
        return performSortedJoin(std::span(leftRelation), std::span(rightRelation));
    } else {
        std::jthread t1(sortCastRelation, leftRelation.begin(), leftRelation.end());
        std::jthread t2(sortTitleRelation, rightRelation.begin(), rightRelation.end());
        t1.join();
        t2.join();
    }
    std::cout << "Relations sorted!\n";

    std::vector<std::jthread> threads;
    std::vector<std::vector<ResultRelation>> resultVectors(numThreads);
    auto chunkStart = leftRelation.begin();
    for(int i = 0; i < numThreads; ++i) {
        std::vector<CastRelation>::iterator chunkEnd;
        if(i == (numThreads - 1)) {
            chunkEnd = leftRelation.end();
        } else {
            chunkEnd = std::next(chunkStart, chunkSize);
        }
        std::span<CastRelation> chunkSpan(std::to_address(chunkStart), std::to_address(chunkEnd));
        threads.emplace_back(processChunk, chunkSpan, std::span(rightRelation), std::ref(resultVectors[i]), std::ref(m_results));
        chunkStart = chunkEnd;
    }
    for(auto& t: threads) {
        t.join();
    }
    std::cout << "All threads joined!\n";
    // Join ResultVectors
    mergeVectors(resultVectors, results);
    return results;
}



std::vector<ResultRelation> performJoin(const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation) {
    std::vector<CastRelation> castRelation(leftRelation);
    std::vector<TitleRelation> titleRelation(rightRelation);
    sortCastRelation(castRelation.begin(), castRelation.end());
    sortTitleRelation(titleRelation.begin(), titleRelation.end());

    return performSortedJoin(castRelation, titleRelation);
}






TEST(ParallelizationTest, TestJoiningTuples) {
    const auto leftRelation = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_uniform.csv"));
    const auto rightRelation = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform.csv"));




    Timer timer("Parallelized Join execute");
    timer.start();

    auto resultTuples = performJoin(leftRelation, rightRelation);

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

    sortCastRelation(leftRelation.begin(), leftRelation.end());
    sortTitleRelation(rightRelation.begin(), rightRelation.end());

    auto results = performSortedJoin(leftRelation, rightRelation);

    for(const auto& record: results) {
        std::cout << resultRelationToString(record) << '\n';
    }
    std::cout << "\n\n";
}
