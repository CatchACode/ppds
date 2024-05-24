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
    results.reserve(845626);
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
    std::cout << "You need to reserve " << results.size() << std::endl;
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
        case HashJoinType::SHJ_MAP: {
            return performSHJ_MAP(leftRelation, rightRelation);
        }
        case HashJoinType::SHJ_UNORDERED_MAP: {
            return performSHJ_UNORDERED_MAP(leftRelation, rightRelation);
        }

        case HashJoinType::CHJ_MAP:{
            return performCHJ_MAP(leftRelation, rightRelation, numThreads);
        }
    }
    return {};
}

#endif //PPDS_PARALLELISM_HASHJOIN_H
