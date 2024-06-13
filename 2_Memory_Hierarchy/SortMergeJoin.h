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
#include <atomic>



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
std::vector<ResultRelation> performSortMergeJoin(const std::vector<CastRelation>& castRelation, const std::vector<TitleRelation>& titleRelation) {
    std::vector<ResultRelation> results;
    results.reserve(castRelation.size());

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
    std::mutex& m_results;
};


void inline processChunk(const std::span<const CastRelation>& castRelation, const std::span<const TitleRelation>& rightRelation, std::vector<ResultRelation>& results, std::mutex& m_results, std::atomic_size_t& r_index) {
    std::forward_iterator auto r_it = std::ranges::lower_bound(
            rightRelation.begin(), rightRelation.end(), TitleRelation{.titleId = castRelation.begin()->movieId},
            [](const TitleRelation& a, const TitleRelation& b) {
                return a.titleId < b.titleId;
            }
    );

    auto l_it = castRelation.begin();
    int32_t currentId = 0;
    size_t index = 0;
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
                    index = r_index.fetch_add(1, std::memory_order_relaxed);
                    //std::cout << "Index: " << index << std::endl;
                    results[index] = createResultTuple(*l_idx, *r_idx);
                    //std::scoped_lock l_result(m_results);
                    //results.emplace_back(createResultTuple(*l_idx, *r_idx));
                }
            }

        }
    }
}

struct WorkerThreadArgs {
    std::queue<std::span<const CastRelation>>& chunks;
    std::mutex& m_chunks;
    const std::vector<TitleRelation>& titleRelation;
    std::vector<ResultRelation>& results;
    std::mutex& m_results;

    std::condition_variable& cv;
    std::atomic_bool& stop;
    std::atomic_size_t& r_index;

};

void workerThread(WorkerThreadArgs args) {
    while(!args.stop || !args.chunks.empty()) {
        std::unique_lock l_chunks(args.m_chunks);
        args.cv.wait(l_chunks, [&args] {return args.stop || !args.chunks.empty();});
        if(!args.chunks.empty()) {
            auto chunk = args.chunks.front();
            args.chunks.pop();
            l_chunks.unlock();
            processChunk(chunk, args.titleRelation, args.results, args.m_results, args.r_index);
        }
    }
}




std::vector<ResultRelation> performThreadedSortJoin(const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation,
                                                    const int numThreads = std::jthread::hardware_concurrency()) {
    size_t chunkSize = (leftRelation.size() / L2_CACHE_SIZE);

    std::cout << "chunkSize: " << chunkSize << std::endl;
    if (chunkSize == 0) {
        /*
        // L2 size is larger than data size
        std::cout << "performing has join!\n" << std::endl;
        std::cout << "numThreads: " << numThreads << std::endl;
        std::cout << "leftRelation.size(): " << leftRelation.size() << std::endl;
        return performHashJoin(HashJoinType::SHJ_UNORDERED_MAP, leftRelation, rightRelation);
        */
        chunkSize = leftRelation.size() / numThreads;
        std::cout << "chunkSize is now: " << chunkSize << std::endl;
        if(chunkSize == 0) {
            std::cout << "Still too small, performing hash join!" << std::endl;
            return performSHJ_UNORDERED_MAP(leftRelation, rightRelation);
        }
    }

    std::mutex m_results;

    std::vector<ResultRelation> results(rightRelation.size());
    std::atomic_size_t r_index(0);



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
                processChunk, chunkSpan, std::span(rightRelation), std::ref(results), std::ref(m_results), std::ref(r_index));
        chunkStart = chunkEnd;
    }
    for (auto &t: threads) {
        t.join();
    }
    // Join ResultVectors
    std::cout << "results.size(): " << results.size() << std::endl;
    return results;
}
#endif //PPDS_PARALLELISM_SORTMERGEJOIN_H