//
// Created by CatchACode on 15.05.24.
//

#ifndef PPDS_PARALLELISM_NESTEDLOOPJOIN_H
#define PPDS_PARALLELISM_NESTEDLOOPJOIN_H

#include "JoinUtils.hpp"

#include <vector>

enum NestLoopJoinTypes : uint8_t {
    NLJ = 1, ///< Nested loop join
    TNLJ = 2, ///< Multi-Threaded nested loop join
};


std::vector<ResultRelation> performNestedLoopJoin(const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation) {
    std::vector<ResultRelation> results;
    for(auto const& castTuple : leftRelation) {
        for(auto const& titleTuple: rightRelation) {
            if(castTuple.movieId == titleTuple.titleId) {
                results.emplace_back(createResultTuple(castTuple, titleTuple));
            }
        }
    }
    return results;
}

#endif //PPDS_PARALLELISM_NESTEDLOOPJOIN_H
