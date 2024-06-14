//
// Created by CatchACode on 15.05.24.
//

#ifndef PPDS_PARALLELISM_SORTMERGEJOIN_H
#define PPDS_PARALLELISM_SORTMERGEJOIN_H

#include "JoinUtils.hpp"
#include "HashJoin.h"
#include "generated_variables.h"

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


void inline processChunk(const std::span<const CastRelation>& castRelation, const std::span<const TitleRelation>& rightRelation, std::vector<ResultRelation>& results, std::atomic_size_t& r_index) {
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
                    index = r_index.fetch_add(1,std::memory_order_relaxed); // std::memory_order_relaxed);
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
    std::condition_variable& cv;
    std::atomic_bool& stop;
    std::atomic_size_t& r_index;

};

void workerThread(const WorkerThreadArgs& args) {
    while(!args.stop || !args.chunks.empty()) {
        std::unique_lock l_chunks(args.m_chunks);
        args.cv.wait(l_chunks, [&args] {return args.stop || !args.chunks.empty();});
        if(!args.chunks.empty()) {
            auto chunk = args.chunks.front();
            args.chunks.pop();
            l_chunks.unlock();
            processChunk(chunk, args.titleRelation, args.results, args.r_index);
        }
    }
}




std::vector<ResultRelation> performThreadedSortJoin(const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation,
                                                    const int numThreads = std::jthread::hardware_concurrency()) {
    static const std::size_t chunkSize = L2_CACHE_SIZE / sizeof(CastRelation);

    std::vector<ResultRelation> results(leftRelation.size());
    std::atomic_size_t r_index(0);
    std::atomic_bool stop(false);
    std::queue<std::span<const CastRelation>> chunks;
    std::mutex m_chunks;
    std::condition_variable cv_queue;
    std::size_t chunkNum = 0;

    const WorkerThreadArgs args(
            std::ref(chunks),
            std::ref(m_chunks),
            std::ref(rightRelation),
            std::ref(results),
            std::ref(cv_queue),
            std::ref(stop),
            std::ref(r_index)
            );

    std::vector<std::jthread> threads;
    for(int i = 0; i < numThreads; ++i) {
        threads.emplace_back(std::jthread(workerThread, std::ref(args)));
    }

    auto chunkStart = leftRelation.begin();
    auto chunkEnd = leftRelation.begin();
    while(chunkEnd != leftRelation.end()) {
        if(std::distance(chunkEnd, leftRelation.end()) > chunkSize) {
            chunkEnd = std::next(chunkEnd, chunkSize);
        } else {
            chunkEnd = leftRelation.end();
        }
        const std::span<const CastRelation> chunkSpan(std::to_address(chunkStart), std::to_address(chunkEnd));
        {
            std::scoped_lock l_chunks(m_chunks);
            chunks.emplace(chunkSpan);
        }
        cv_queue.notify_one();
        chunkStart = chunkEnd;
        chunkNum++;
    }
    stop.store(true);
    cv_queue.notify_all();
    for (auto &t: threads) {
        t.join();
    }
    // Join ResultVectors
    results.resize(r_index.load()); // r_index also conveniently counts the amount of joined records
    std::cout << "results.size(): " << results.size() << std::endl;
    std::cout << "Created " << chunkNum << " Chunks" << std::endl;

    return results;
}
#endif //PPDS_PARALLELISM_SORTMERGEJOIN_H