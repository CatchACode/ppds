//
// Created by CatchACode on 15.05.24.
//

#ifndef PPDS_PARALLELISM_HASHJOIN_H
#define PPDS_PARALLELISM_HASHJOIN_H


#include <map>
#include <unordered_map>
#include <span>

#include "JoinUtils.hpp"

/**
 * Enum Class to select which type of hash-join to execute
 */

enum class HashJoinType : uint8_t {
    SHJ_MAP = 1, ///< single-threaded-hash-join on a std::map
    SHJ_UNORDERED_MAP = 2, ///< single-threaded-hash-join on a std::unordered_map
    CHJ_MAP = 3, ///< multithreaded-hash-join where the dataset is divide into size / numthreads chunks for the threads
};



std::vector<ResultRelation> performSHJ_MAP(const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation) {
    std::vector<ResultRelation> results;
    // Build HashMap
    std::map<int32_t, const TitleRelation*> map;
    for(const TitleRelation& record: rightRelation) {
        map[record.titleId] = &record;
    }
    for(const CastRelation& castRelation: leftRelation) {
        if(map.contains(castRelation.movieId)) {
            results.emplace_back(createResultTuple(castRelation, *map[castRelation.movieId]));
        }
    }
    return results;
}

std::vector<ResultRelation> performSHJ_UNORDERED_MAP(const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation) {
    std::vector<ResultRelation> results;
    results.reserve(leftRelation.size());
    // Build HashMap
    std::unordered_map<int32_t, const TitleRelation*> map;
    map.reserve(rightRelation.size());
    for(const auto& record: rightRelation) {
        map[record.titleId] = &record;
    }
    for(const auto& record: leftRelation) {
        if(map.contains(record.movieId)) {
            results.emplace_back(createResultTuple(record, *map[record.movieId]));
        }
    }
    return results;
}


std::vector<ResultRelation> perform2THJ(const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation) {
    std::vector<ResultRelation> results;
    results.reserve(leftRelation.size());
    // Divide rightRelation into two
    const std::span<const TitleRelation> span1(std::to_address(rightRelation.begin()), std::to_address(rightRelation.begin() + (rightRelation.size() / 2)));
    const std::span<const TitleRelation> span2(std::to_address(rightRelation.begin()) + (rightRelation.size() / 2), std::to_address(rightRelation.end()));
    std::atomic_size_t results_index;
    auto join_half = [&results, &results_index, &leftRelation](const std::span<const TitleRelation>& span) {
        std::unordered_map<int32_t, const TitleRelation*> map;
        map.reserve(span.size());
        // Build map
        std::ranges::for_each(span, [&map](const TitleRelation& record){map[record.titleId] = &record;});
        std::ranges::for_each(leftRelation, [&map, &results, &results_index](const CastRelation& record) {
           if(map.contains(record.movieId)) {
               results.emplace(results.begin() + results_index++, createResultTuple(record, *map[record.movieId]));
           }
        });
    };
    std::jthread otherHalf(join_half, std::ref(span1));
    join_half(span2);
    otherHalf.join();
    return results;
}

std::vector<ResultRelation> performCHJ_MAP(const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation, const int numThreads = std::jthread::hardware_concurrency()) {
    const size_t chunkSize = rightRelation.size() / numThreads;

    std::vector<ResultRelation> results;
    results.reserve(leftRelation.size());
    std::mutex m_results;

    std::vector<std::jthread> threads;


    auto chunkStart = rightRelation.begin();
    for(int i = 0; i < numThreads; ++i) {
        std::vector<TitleRelation>::const_iterator chunkEnd;
        if (i == (numThreads - 1)) {
            chunkEnd = rightRelation.end();
        } else {
            chunkEnd = std::next(chunkStart, chunkSize);
        }
        const std::span<const TitleRelation> chunkSpan(std::to_address(chunkStart), std::to_address(chunkEnd));
        threads.emplace_back([&results, &m_results, chunkSpan, &leftRelation] {
            // Build HashMap
            std::unordered_map<int32_t, const TitleRelation*> map;
            map.reserve(chunkSpan.size());
            for(const auto& record: chunkSpan) {
                map[record.titleId] = &record;
            }
            for(const auto& record : leftRelation) {
                if(map.contains(record.movieId)) {
                    std::lock_guard l_results(m_results);
                    results.emplace_back(createResultTuple(record, *map[record.movieId]));
                }
            }
        });
        chunkStart = chunkEnd;
    }
    for(auto& thread: threads) {
        thread.join();
    }
    std::cout << "results size:" << results.size() << std::endl;
    return results;

}

struct ThreadArgs {
    unsigned int threadId;
    std::queue<std::span<const TitleRelation>>& chunks;
    std::mutex& m_chunks;
    std::condition_variable& cv_queue;
    std::vector<ResultRelation>& results;
    std::mutex& m_results;
    const std::vector<CastRelation>& leftRelation;
    std::atomic_bool& stop;
    std::size_t& result_index;
};

void workerThreadChunk(ThreadArgs args) {
    std::cout << "Thread started" << std::endl;
    while(!args.stop || !args.chunks.empty()) {
        std::unique_lock l_chunks(args.m_chunks);
        args.cv_queue.wait(l_chunks, [&args] {return args.stop || !args.chunks.empty();});
        if(!args.chunks.empty()) {
            auto chunk = args.chunks.front();
            args.chunks.pop();
            l_chunks.unlock();
            // Build HashMap
            std::unordered_map<int32_t, const TitleRelation*> map;
            map.reserve(chunk.size());
            std::ranges::for_each(chunk, [&map](const TitleRelation& record) {map[record.titleId] = &record;});
            // Probe HashMap
            std::ranges::for_each(args.leftRelation, [&map, &args](const CastRelation& record) {
                if(map.contains(record.movieId)) {
                    std::lock_guard l_results(args.m_results);
                    args.results.emplace_back(createResultTuple(record, *map[record.movieId]));
                }
            });
        }
    }
    std::cout << "Thread done!" << std::endl;
    std::cout << std::flush;
}

std::vector<ResultRelation> performCacheSizedThreadedHashJoin(const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation, const unsigned int numThreads = std::jthread::hardware_concurrency()) {
    if(numThreads == 1) {
        return performSHJ_UNORDERED_MAP(leftRelation, rightRelation);
    }
    size_t hashmap_size = HASHMAP_SIZE;
    if(hashmap_size > rightRelation.size() / numThreads) {
        hashmap_size = (rightRelation.size() / numThreads == 0) ? 1 : rightRelation.size() / numThreads + 1;
        std::cout << "Hash map size changed to " << hashmap_size << std::endl;
    }
    std::vector<ResultRelation> results;
    std::mutex m_results;
    std::vector<std::jthread> threads;
    std::atomic_bool stop = false;
    std::queue<std::span<const TitleRelation>> chunks;
    std::mutex m_chunks;
    std::condition_variable cv_queue;
    results.reserve(leftRelation.size());
    size_t numChunks = 0;
    std::size_t results_index(0);

    for(unsigned int i = 0; i < numThreads; ++i) {
        threads.emplace_back(workerThreadChunk, ThreadArgs(i, std::ref(chunks), std::ref(m_chunks),
                                                                             std::ref(cv_queue), std::ref(results),
                                                                             std::ref(m_results), std::ref(leftRelation),
                                                                             std::ref(stop), std::ref(results_index)));
    };

    auto chunkStart = rightRelation.begin();
    auto chunkEnd = rightRelation.begin();
    while(chunkEnd != rightRelation.end()) {
        if(std::distance(chunkEnd, rightRelation.end()) > (long long int)hashmap_size) {
            chunkEnd = std::next(chunkEnd, (long long int)hashmap_size);
            std::cout << "Created chunk with size: " << std::distance(chunkStart, chunkEnd) << std::endl;
        } else {
            chunkEnd = rightRelation.end();
            std::cout << "Created chunk with size: " << std::distance(chunkStart, chunkEnd) << std::endl;
        }
        const std::span<const TitleRelation> chunkSpan(std::to_address(chunkStart), std::to_address(chunkEnd));
        {
            std::scoped_lock l_chunks(m_chunks);
            chunks.emplace(chunkSpan);
            cv_queue.notify_one();
        }
        ++numChunks;
        chunkStart = chunkEnd;
    }

    stop.store(true);
    cv_queue.notify_all();
    for(auto& thread: threads) {
        thread.join();
    }
    std::cout << "Created " << numChunks << " Chunks" << std::endl;
    return results;
}

/** Has to remain at the bottom!
 *
 * @param joinType
 * @param leftRelation
 * @param rightRelation
 * @return
 */

std::vector<ResultRelation> performHashJoin(enum HashJoinType joinType, const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation, const int numThreads = std::jthread::hardware_concurrency()) {
    switch (joinType) {
        using enum HashJoinType;
        case SHJ_MAP: {
            return performSHJ_MAP(leftRelation, rightRelation);
        }
        case SHJ_UNORDERED_MAP: {
            return performSHJ_UNORDERED_MAP(leftRelation, rightRelation);
        }

        case CHJ_MAP:{
            return performCHJ_MAP(leftRelation, rightRelation, numThreads);
        }
    }
    return {};
}

#endif //PPDS_PARALLELISM_HASHJOIN_H
