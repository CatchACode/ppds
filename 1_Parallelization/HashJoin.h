//
// Created by CatchACode on 15.05.24.
//

#ifndef PPDS_PARALLELISM_HASHJOIN_H
#define PPDS_PARALLELISM_HASHJOIN_H


#include <map>
#include <unordered_map>
#include "JoinUtils.hpp"

/**
 * Enum Class to select which type of hash-join to execute
 */

enum HashJoinType : uint8_t {
    SHJ_MAP = 1, ///< single-threaded-hash-join on a std::map
    SHJ_UNORDERED_MAP = 2, ///< single-threaded-hash-join on a std::unordered_map
};



std::vector<ResultRelation> performSHJ_MAP(const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation) {
    std::vector<ResultRelation> results;
    // Build HashMap
    std::map<int32_t, TitleRelation> map;
    for(const TitleRelation& record: rightRelation) {
        map[record.titleId] = record;
    }
    for(const CastRelation& castRelation: leftRelation) {
        if(map.contains(castRelation.movieId)) {
            results.emplace_back(createResultTuple(castRelation, map[castRelation.movieId]));
        }
    }
    return results;
}

std::vector<ResultRelation> performSHJ_UNORDERED_MAP(const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation) {
    std::vector<ResultRelation> results;
    // Build HashMap
    std::unordered_map<int32_t, TitleRelation> map;
    for(const TitleRelation& record: rightRelation) {
        map[record.titleId] = record;
    }
    for(const CastRelation& castRelation: leftRelation) {
        if(map.contains(castRelation.movieId)) {
            results.emplace_back(createResultTuple(castRelation, map[castRelation.movieId]));
        }
    }
    return results;
}

std::vector<ResultRelation> performCHJ_MAP(const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation, int numThreads = std::jthread::hardware_concurrency()) {
    return {};
}





/** Has to remain at the both!
 *
 * @param joinType
 * @param leftRelation
 * @param rightRelation
 * @return
 */

std::vector<ResultRelation> performHashJoin(enum HashJoinType joinType, const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation) {
    switch (joinType) {
        case SHJ_MAP: {
            return performSHJ_MAP(leftRelation, rightRelation);
        }
        case SHJ_UNORDERED_MAP: {
            return performSHJ_UNORDERED_MAP(leftRelation, rightRelation);
        }
    }
}

#endif //PPDS_PARALLELISM_HASHJOIN_H
