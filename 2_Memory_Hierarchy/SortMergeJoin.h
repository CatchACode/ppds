//
// Created by CatchACode on 15.05.24.
//

#ifndef PPDS_PARALLELISM_SORTMERGEJOIN_H
#define PPDS_PARALLELISM_SORTMERGEJOIN_H

#include "JoinUtils.hpp"
#include "HashJoin.h"
#include "generated_variables.h"
#include "CustomAllocator.h"

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
#include <list>


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

struct ChunkCastRelation {
    const std::vector<CastRelation>::const_iterator start;
    const std::vector<CastRelation>::const_iterator end;
};

struct ChunkTitleRelation {
    const std::vector<TitleRelation>::const_iterator start;
    const std::vector<TitleRelation>::const_iterator end;
};

void inline processChunk(const ChunkCastRelation& chunkCastRelation, const ChunkTitleRelation& chunkTitleRelation, std::vector<ResultRelation>& results,
                        std::atomic_size_t& r_index, std::mutex& m_results) {
    std::forward_iterator auto r_it = std::ranges::lower_bound(
            chunkTitleRelation.start, chunkTitleRelation.end,
            TitleRelation{.titleId = chunkCastRelation.start->movieId},
            [](const TitleRelation &a, const TitleRelation &b) {
                return a.titleId < b.titleId;
            }
    );
    std::forward_iterator auto l_it = chunkCastRelation.start;
    int32_t currentId = 0;
    size_t index = 0;
    while (l_it != chunkCastRelation.end && r_it != chunkTitleRelation.end) {
        r_it = std::ranges::lower_bound(
                r_it, chunkTitleRelation.end,
                TitleRelation{.titleId = l_it->movieId},
                [](const TitleRelation &a, const TitleRelation &b) {
                    return a.titleId < b.titleId;
                }
        );

        if (l_it->movieId < r_it->titleId) {
            ++l_it;
        } else if (l_it->movieId > r_it->titleId) {
            ++r_it;
        } else {
            auto r_start = r_it;
            auto l_start = l_it;
            currentId = r_it->titleId;

            // Find End of block where both sides share keys
            while (r_it != chunkTitleRelation.end && r_it->titleId == currentId) {
                ++r_it;
            }
            while (l_it != chunkCastRelation.end && l_it->movieId == currentId) {
                ++l_it;
            }
            //size_t matchingLeft = std::distance(l_start, l_it);
            //size_t matchingRight = std::distance(r_start, r_it);
            //index = r_index.fetch_add(matchingLeft*matchingRight,std::memory_order_relaxed);
            std::scoped_lock l_results(m_results);
            for (std::forward_iterator auto l_idx = l_start; l_idx != l_it; ++l_idx) {
                for (std::forward_iterator auto r_idx = r_start; r_idx != r_it; ++r_idx) {
                    //results[index++] = createResultTuple(*l_idx, *r_idx);
                    results.emplace_back(createResultTuple(*l_idx, *r_idx));
                }
            }
        }
    }
}

struct WorkerThreadArgs {
    std::vector<ChunkCastRelation>& chunks;
    std::mutex& m_chunks;
    const ChunkTitleRelation titleRelation;
    std::vector<ResultRelation>& results;
    std::mutex& m_results;
    std::condition_variable& cv;
    std::atomic_bool& stop;
    std::atomic_size_t& r_index;

};

void workerThread(const WorkerThreadArgs& args) {
    while(!args.stop.load(std::memory_order_relaxed) || !args.chunks.empty()) {
        std::unique_lock l_chunks(args.m_chunks);
        args.cv.wait(l_chunks, [&args] {return args.stop.load(std::memory_order_relaxed) || !args.chunks.empty();});
        if(!args.chunks.empty()) {
            auto chunkCastRelation = std::move(args.chunks.back());
            args.chunks.pop_back();
            l_chunks.unlock();
            processChunk(chunkCastRelation, args.titleRelation,args.results, args.r_index, args.m_results);
        }
    }
    //std::cout << "Stopping thread\n";
}


std::vector<ResultRelation> performThreadedSortJoin(const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation,
                                                    const unsigned int numThreads = std::jthread::hardware_concurrency()) {
    if(leftRelation.size() < 20000) {
        return performSortMergeJoin(leftRelation, rightRelation);
    }
    int32_t maxTitleId = rightRelation[rightRelation.size()].titleId;
    int32_t minTitleId = rightRelation[0].titleId;
    int32_t maxMovieId = leftRelation[leftRelation.size()].movieId;
    int32_t minMovieId = leftRelation[0].movieId;
    std::cout << "CastRelation Min: " << minMovieId << " Max: " << maxMovieId << '\n';
    std::cout << "TitleRelation Min: " << minTitleId << " Max: " << maxTitleId << '\n';

    const std::size_t chunkSize = (leftRelation.size() / numThreads) > 0 ? leftRelation.size() / numThreads: leftRelation.size();
    //std::vector<ResultRelation> results(leftRelation.size());
    //std::cout << "Initialized results with a size of " << leftRelation.size() << " | size: " << results.size() << std::endl;
    std::vector<ResultRelation> results;
    //results.reserve(10);
    //results.reserve(leftRelation.size() > rightRelation.size() ? leftRelation.size() : rightRelation.size());
    std::mutex m_results;
    std::atomic_size_t r_index(0);
    std::atomic_bool stop(false);
    std::vector<ChunkCastRelation> chunks;
    //chunks.reserve(100);
    //chunks.reserve(200);
    std::mutex m_chunks;
    std::condition_variable cv_queue;
    //std::size_t chunkNum = 0;
    std::size_t maxChunksInQueue = 0;

    const WorkerThreadArgs args(
            std::ref(chunks),
            std::ref(m_chunks),
            ChunkTitleRelation(rightRelation.begin(), rightRelation.end()),
            std::ref(results),
            std::ref(m_results),
            std::ref(cv_queue),
            std::ref(stop),
            std::ref(r_index)
            );

    std::vector<std::jthread> threads;
    threads.reserve(numThreads);
    for(unsigned int i = 0; i < numThreads; ++i) {
        threads.emplace_back(workerThread, std::ref(args));
    }

    auto chunkStart = leftRelation.begin();
    auto chunkEnd = leftRelation.begin();
    while(chunkEnd != leftRelation.end()) {
        if((long unsigned int)std::distance(chunkEnd, leftRelation.end()) > chunkSize) {
            chunkEnd = std::next(chunkEnd, chunkSize);
        } else {
            chunkEnd = leftRelation.end();
        }
        {
            std::scoped_lock l_chunks(m_chunks);
            chunks.emplace_back(chunkStart, chunkEnd);
            //std::cout << "chunks.size()" << chunks.size() << std::endl;
            //maxChunksInQueue = maxChunksInQueue < chunks.size() ? chunks.size() : maxChunksInQueue;
        }
        cv_queue.notify_one();
        chunkStart = chunkEnd;
        //chunkNum++;
    }
    stop.store(true, std::memory_order_relaxed);
    //std::cout << "All threads should stop now!\n";
    cv_queue.notify_all();
    for (auto &t: threads) {
        t.join();
    }
    //std::cout <<"\n\nFinished!\n\n";
    // Join ResultVectors
    //std::cout << "results.size() before resize: " << results.size() << std::endl;
    //std::cout << "results[0] = " << resultRelationToString(results[0]) << std::endl;
    //std::cout << "results[results.size()-1] = " << resultRelationToString(results[results.size() - 1]) << std::endl;
    //results.resize(r_index.load()); // r_index also conveniently counts the amount of joined records
    //std::cout << "results.size(): " << results.size() << std::endl;
    //std::cout << "Created " << chunkNum << " Chunks" << std::endl;
    //std::cout << "results.size(): " << results.size() << std::endl;
    //std::cout << "Max chunks in Queue: " << maxChunksInQueue << std::endl;
    //std::cout << "r_index: " << r_index.load() << std::endl;
    //std::cout << resultRelationToString(results[0]) << std::endl;
    //std::cout << resultRelationToString(results[results.size()-1]) << std::endl;

    return results;
}
#endif //PPDS_PARALLELISM_SORTMERGEJOIN_H