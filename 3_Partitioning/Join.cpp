/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

//#include "TimerUtil.hpp"
#include "JoinUtils.hpp"
#include <vector>
#include <span>
#include <unordered_map>
#include <gtest/gtest.h>
#include <mutex>
#include "Partitioning.h"
#include "TimerUtil.hpp"
#include "generated_variables.h"

void inline buildMap(const std::span<const TitleRelation*>& titleSpan, std::unordered_map<int32_t, const TitleRelation*>& map) {
    //#pragma omp parallel for
    for(const auto& record: titleSpan) {
        //#pragma omp critical
        map[record->titleId] = record;
    }
}

void probeMap(const std::span<const CastRelation*> titleSpan, std::unordered_map<int32_t, const TitleRelation*>& map,
              std::vector<ResultRelation>& results) {
    std::vector<std::pair<const CastRelation*, const TitleRelation*>> localResults;
    #pragma omp parallel for
    for(const auto& record:titleSpan) {
        auto it = map.find(record->movieId);
        if(it != map.end()) {
            #pragma omp critical
            localResults.emplace_back(record, it->second);
        }
    }
    #pragma omp parallel for
    for(const auto&[castPtr, titlePtr]: localResults) {
        #pragma omp critical
        results.emplace_back(createResultTuple(*castPtr, *titlePtr));
    }
}

constexpr const size_t MAX_HASHMAP_SIZE = L2_CACHE_SIZE / (sizeof(int32_t) + sizeof(CastRelation*));


std::vector<ResultRelation> performJoin(const std::vector<CastRelation>& castRelation, const std::vector<TitleRelation>& titleRelation, int numThreads) {
    auto [castPartitionIndexes, castPartitioned] = radixCastPartition(castRelation, numThreads);
    auto [titlePartitionIndexes, titlePartitioned] = radixTitlePartition(titleRelation, numThreads);

    std::vector<ResultRelation> results;
    results.reserve(castRelation.size());
    for(int i = 0; i < castPartitionIndexes.size(); ++i) {
        auto castSpanStart = castPartitioned.begin() + castPartitionIndexes[i];
        auto castSpanEnd =  (i == castPartitionIndexes.size() - 1 ? castPartitioned.end(): castPartitioned.begin() + castPartitionIndexes[i+1]);
        std::span<const CastRelation*> castSpan(castSpanStart, castSpanEnd);

        auto titleSpanStart = titlePartitioned.begin() + titlePartitionIndexes[i];
        auto titleSpanEnd = (i == titlePartitionIndexes.size() - 1 ? titlePartitioned.end(): titlePartitioned.begin() + titlePartitionIndexes[i+1]);
        std::span<const TitleRelation*> titleSpan(titleSpanStart, titleSpanEnd);

        size_t numChunks = (titleSpan.size() / MAX_HASHMAP_SIZE);

        //#pragma omp parallel for
        for(int i = 0; i < numChunks; ++i) {
            std::unordered_map<int32_t, const TitleRelation*> map;
            map.reserve(MAX_HASHMAP_SIZE);
            auto chunkStart = titleSpan.begin() + i*MAX_HASHMAP_SIZE;
            auto chunkEnd = chunkStart + std::min(std::distance(chunkStart, chunkStart + MAX_HASHMAP_SIZE), std::distance(chunkStart, titleSpan.end()));
            auto chunk = std::span<const TitleRelation*>(chunkStart, chunkEnd);
            buildMap(chunk, map);
            probeMap(castSpan, map, results);
            map.clear();
        }
    }
    std::cout << "results.size(): " << results.size() << '\n';
    return std::move(results);
}


TEST(PartitioninJoinTest, TestJoin) {
    const auto castRelation = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_zipfian1gb.csv"));
    const auto titleRelation = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform1gb.csv"));

    Timer timer("Join");
    timer.start();
    auto results = performJoin(castRelation, titleRelation, 8);
    timer.pause();
    std::cout << "Timer: " << printString(timer) << std::endl;
    /*
    for(const auto& result: results) {
        std::cout << resultRelationToString(result) << '\n';
    }
     */
}


TEST(PartitionJoinTest, NestedLoopJoin) {
    const auto castRelation = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_zipfian1kb.csv"));
    const auto titleRelation = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform1kb.csv"));

    std::vector<ResultRelation> results;
    for(const auto& castTuple: castRelation) {
        for(const auto& titleTuple: titleRelation) {
            if(castTuple.movieId == titleTuple.titleId) {
                results.emplace_back(createResultTuple(castTuple, titleTuple));
            }
        }
    }
    std::cout << "results.size(): " << results.size() << '\n';
}
