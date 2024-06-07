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

#include "TimerUtil.hpp"
#include "JoinUtils.hpp"
#include <unordered_map>
#include <iostream>
#include <gtest/gtest.h>
#include <omp.h>

#include "HashJoin.h"

std::vector<ResultRelation> performJoin(const std::vector<CastRelation>& castRelation, const std::vector<TitleRelation>& titleRelation, int numThreads) {
    std::vector<ResultRelation> results;
    results.reserve(castRelation.size());
    for(const auto& castRecord: castRelation) {
        for(const auto& titleRecord: titleRelation) {
            if(castRecord.movieId == titleRecord.titleId) {
                results.emplace_back(createResultTuple(castRecord, titleRecord));
            }
        }
    }
}


TEST(MemoryHierarchyTest, TestJoiningTuples) {
    const auto leftRelation = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_uniform.csv"));
    const auto rightRelation = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform.csv"));
    auto results = performJoin(leftRelation, rightRelation, 8);

    std::cout << "Result size: " << results.size() << std::endl;
    std::cout << "\n\n";
}


