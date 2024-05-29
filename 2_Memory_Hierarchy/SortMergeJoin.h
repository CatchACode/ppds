//
// Created by CatchACode on 15.05.24.
//

#ifndef PPDS_PARALLELISM_SORTMERGEJOIN_H
#define PPDS_PARALLELISM_SORTMERGEJOIN_H

#include "JoinUtils.hpp"
#include "HashJoin.h"

#include <span>
#include <thread>
#include <ranges>
#include <algorithm>
#include <execution>
#include <iterator>
#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <omp.h>



enum class SortMergeJoinType : uint8_t {
    SMJ = 1, ///< single threaded sort merge join
    TSMJ = 2, ///< multi-threaded sort merge join, where the leftRelation is divided in chunks by the number of threads
    TSMJv2 = 3, ///< multi-threaded sort merge join,
};


/** performs a sorted join, <b>has undefined behaviour if the spans are not sorted!</b>
 *
 * @param castRelation a sorted span of cast records
 * @param titleRelation a sorted span of title records
 * @return a std::vector<ResultRelation> of joined tuples
 */
std::vector<ResultRelation> performSortMergeJoin(const std::span<CastRelation>& castRelation, const std::span<TitleRelation>& titleRelation) {
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

struct threadArgs {
    std::span<const CastRelation> castRelation;
    std::span<const TitleRelation> rightRelation;
    std::vector<ResultRelation>& results;
    bool& sorted;
    std::condition_variable& cv_sorted;
    std::mutex& m_sorted;
    std::mutex& m_results;
};


void processChunk(std::unique_ptr<threadArgs> args) {
    std::forward_iterator auto r_it = std::ranges::lower_bound(
            args->rightRelation.begin(), args->rightRelation.end(), TitleRelation{.titleId = args->castRelation.begin()->movieId},
            [](const TitleRelation& a, const TitleRelation& b) {
                return a.titleId < b.titleId;
            }
    );

    auto l_it = args->castRelation.begin();
    int32_t currentId = 0;
    while(l_it != args->castRelation.end() && r_it != args->rightRelation.end()) {
        if(l_it->movieId < r_it->titleId) {
            ++l_it;
        } else if (l_it->movieId > r_it->titleId) {
            ++r_it;
        } else {
            std::forward_iterator auto r_start = r_it;
            std::forward_iterator auto l_start = l_it;
            currentId = r_it->titleId;

            // Find End of block where both sides share keys
            while(r_it != args->rightRelation.end() && r_it->titleId == currentId) {
                ++r_it;
            }
            while(l_it != args->castRelation.end() && l_it->movieId == currentId) {
                ++l_it;
            }

            std::lock_guard lock(args->m_results);
            for(std::forward_iterator auto l_idx = l_start; l_idx != l_it; ++l_idx) {
                for(std::forward_iterator auto r_idx = r_start; r_idx != r_it; ++r_idx) {
                    args->results.emplace_back(createResultTuple(*l_idx, *r_idx));
                }
            }

        }
    }
}

std::vector<ResultRelation> performThreadedSortJoin(const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation,
                                                    const int numThreads = std::jthread::hardware_concurrency()) {
    size_t chunkSize = leftRelation.size() / numThreads;
    if (chunkSize == 0) {
        // numThreads is larger than data size
        return performHashJoin(HashJoinType::SHJ_UNORDERED_MAP, leftRelation, rightRelation);
    }

    bool sorted = false;
    std::condition_variable cv_sorted;
    std::mutex m_sorted;
    std::mutex m_results;

    std::vector<ResultRelation> results;
    results.reserve(rightRelation.size());



    std::vector<std::jthread> threads;
    auto chunkStart = leftRelation.begin();
    for (int i = 0; i < numThreads; ++i) {
        std::vector<CastRelation>::const_iterator chunkEnd;
        if (i == (numThreads - 1)) {
            chunkEnd = leftRelation.end();
        } else {
            chunkEnd = std::next(chunkStart, chunkSize);
        }
        std::span<const CastRelation> chunkSpan(std::to_address(chunkStart), std::to_address(chunkEnd));
        threads.emplace_back(
                processChunk, std::move(std::make_unique<threadArgs>(chunkSpan, rightRelation, results, sorted, cv_sorted, m_sorted, m_results)));
        chunkStart = chunkEnd;
    }
    for (auto &t: threads) {
        t.join();
    }
    // Join ResultVectors
    return results;
}
#endif //PPDS_PARALLELISM_SORTMERGEJOIN_H