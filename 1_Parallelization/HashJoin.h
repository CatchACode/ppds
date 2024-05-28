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
    // Build HashMap
    std::unordered_map<int32_t, const TitleRelation*> map;
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
            std::unordered_map<int32_t, const TitleRelation*> map;
            map.reserve(chunkSpan.size());
            for(const TitleRelation& record: chunkSpan) {
                map[record.titleId] = &record;
            }
            for(const CastRelation& record: leftRelation) {
                if(map.contains(record.movieId)) {
                    std::scoped_lock lock(m_results);
                    results.emplace_back(createResultTuple(record, *map[record.movieId]));
                }
            }
        });
        chunkStart = chunkEnd;
    }
    for(auto& thread: threads) {
        thread.join();
    }
    return results;

}
static const size_t HASHMAP_SIZE = L2_CACHE_SIZE / (sizeof(CastRelation*) + sizeof(int32_t));

struct ThreadArgs {
    int threadId;
    std::queue<std::span<const TitleRelation>>& chunks;
    std::mutex& m_chunks;
    std::condition_variable& cv_queue;
    std::vector<ResultRelation>& results;
    std::mutex& m_results;
    const std::vector<CastRelation>& leftRelation;
    std::atomic_bool& stop;
    std::atomic_size_t& numChunks;
};

void workerThreadChunk(std::unique_ptr<ThreadArgs> args) {
    size_t chunkNum = 0;
    while(!args.get()->stop || !args.get()->chunks.empty()) {
        std::unique_lock l_chunks(args.get()->m_chunks);
        args.get()->cv_queue.wait(l_chunks, [&args] {return args->stop || !args->chunks.empty();});
        if(!args->chunks.empty()) {
            auto chunk = args.get()->chunks.front();
            args->chunks.pop();
            l_chunks.unlock();

            std::unordered_map<int32_t, const TitleRelation*> map;
            map.reserve(chunk.size());

            for(const TitleRelation& record: chunk) {
                map[record.titleId] = &record;
            }

            for(const CastRelation& record: args->leftRelation) {
                if(map.contains(record.movieId)) {
                    std::scoped_lock l_results(args.get()->m_results);
                    args->results.emplace_back(createResultTuple(record, *map[record.movieId]));
                }
            }
        }
    }
}

std::vector<ResultRelation> performCacheSizedThreadedHashJoin(const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation, const int numThreads = std::jthread::hardware_concurrency()) {

    std::vector<ResultRelation> results;
    std::mutex m_results;
    std::vector<std::jthread> threads;
    std::atomic_bool stop = false;
    std::queue<std::span<const TitleRelation>> chunks;
    std::mutex m_chunks;
    std::condition_variable cv_queue;
    std::atomic_size_t numChunks = 0;
    std::cout << "HashMapSIze: " << HASHMAP_SIZE << std::endl;
    results.reserve(leftRelation.size());

    for(int i = 0; i < numThreads; ++i) {
        threads.emplace_back(std::jthread(workerThreadChunk, std::make_unique<ThreadArgs>(i, std::ref(chunks), std::ref(m_chunks),
                                                                                          std::ref(cv_queue), std::ref(results),
                                                                                          std::ref(m_results), std::ref(leftRelation),
                                                                                          std::ref(stop), std::ref(numChunks))));
    };

    auto chunkStart = rightRelation.begin();
    auto chunkEnd = rightRelation.begin();
    while(chunkEnd != rightRelation.end()) {
        if(std::distance(chunkEnd, rightRelation.end()) > HASHMAP_SIZE) {
            chunkEnd = std::next(chunkEnd, HASHMAP_SIZE);
        } else {
            chunkEnd = rightRelation.end();
        }
        const std::span<const TitleRelation> chunkSpan(std::to_address(chunkStart), std::to_address(chunkEnd));
        {
            std::scoped_lock l_chunks(m_chunks);
            chunks.emplace(chunkSpan);
            cv_queue.notify_one();
        }
        chunkStart = chunkEnd;
    }


    stop.store(true);
    cv_queue.notify_all();
    for(auto& thread: threads) {
        thread.join();
    }
    return results;
}

/** Has to remain at the both!
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
