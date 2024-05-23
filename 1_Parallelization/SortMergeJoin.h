//
// Created by CatchACode on 15.05.24.
//

#ifndef PPDS_PARALLELISM_SORTMERGEJOIN_H
#define PPDS_PARALLELISM_SORTMERGEJOIN_H

#include "JoinUtils.hpp"
#include "NestedLoopJoin.h"
#include "MergeSort.h"

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

void processChunk(const std::span<CastRelation> castRelation, std::span<TitleRelation> rightRelation,
                  std::vector<ResultRelation>& results, bool& sorted, std::condition_variable& cv_sorted, std::mutex& m_sorted) {
    std::ranges::sort(castRelation.begin(), castRelation.end(), compareCastRelations);
    std::forward_iterator auto r_it = std::ranges::lower_bound(
            rightRelation.begin(), rightRelation.end(), TitleRelation{.titleId = castRelation.begin()->movieId},
            [](const TitleRelation& a, const TitleRelation& b) {
                return a.titleId < b.titleId;
            }
    );
    std::unique_lock lock(m_sorted);
    cv_sorted.wait(lock, [&sorted]{return sorted;});
    std::cout << "TitleRelation should be sorted!\n";

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
                    results.emplace_back(createResultTuple(*l_idx, *r_idx));
                }
            }

        }
    }
    //std::cout << std::this_thread::get_id() << ": has finished it's chunk\n";
}

void mergeVectors(const std::vector<std::vector<ResultRelation>>& resultVectors, std::vector<ResultRelation>& results) {
    size_t size = 0;
    for(const auto& vector : resultVectors) {
        size += vector.size();
    }
    results.reserve(size);
    for(const auto& vector: resultVectors) {
        std::ranges::move(vector.begin(), vector.end(), std::back_inserter(results));
    }
}

std::vector<ResultRelation> performThreadedSortJoin(const std::vector<CastRelation>& leftRelationConst, const std::vector<TitleRelation>& rightRelationConst,
                                                    const int numThreads = std::jthread::hardware_concurrency()) {
    ThreadPool threadPool(numThreads);
    size_t chunkSize = leftRelationConst.size() / numThreads;
    if (chunkSize == 0) {
        // numThreads is larger than data size
        return performNestedLoopJoin(leftRelationConst, rightRelationConst);
    }

    std::vector<CastRelation> leftRelation(leftRelationConst);
    std::vector<TitleRelation> rightRelation(rightRelationConst);
    bool sorted = false;
    std::condition_variable cv_sorted;
    std::mutex m_sorted;

    auto t2 = std::jthread([&rightRelation, &sorted, &cv_sorted] {
        std::sort(rightRelation.begin(), rightRelation.end(), compareTitleRelations);
        sorted = true;
        cv_sorted.notify_all();
        std::cout << "TitleRelation is sorted!\n";
    });
    //t1.join();
    //t2.join(); // Make all threads wait until t2 finishes, but waiting to join here stalls the dividing chunks -> therefore stalls sorting the chunks!
    std::vector<ResultRelation> results;


    //std::cout << "Relations sorted!\n";

    std::vector<std::jthread> threads;
    std::vector<std::vector<ResultRelation>> resultVectors(numThreads);
    auto chunkStart = leftRelation.begin();
    for (int i = 0; i < numThreads; ++i) {
        std::vector<CastRelation>::iterator chunkEnd;
        if (i == (numThreads - 1)) {
            chunkEnd = leftRelation.end();
        } else {
            chunkEnd = std::next(chunkStart, chunkSize);
        }
        std::span<CastRelation> chunkSpan(std::to_address(chunkStart), std::to_address(chunkEnd));
        threads.emplace_back(processChunk, chunkSpan, std::span(rightRelation), std::ref(resultVectors[i]),
                             std::ref(sorted), std::ref(cv_sorted), std::ref(m_sorted));
        chunkStart = chunkEnd;
    }
    t2.join();
    for (auto &t: threads) {
        t.join();
    }
    //std::cout << "All threads joined!\n";
    // Join ResultVectors
    mergeVectors(resultVectors, results);
    return results;
}
#endif //PPDS_PARALLELISM_SORTMERGEJOIN_H