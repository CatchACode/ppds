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

#include <iostream>
#include <gtest/gtest.h>

#include "NestedLoopUtils.hpp"


//==--------------------------------------------------------------------==//
//==------------------- TODO: JOIN IMPLEMENTATION ----------------------==//
//==--------------------------------------------------------------------==//

std::vector<ResultRelation> performNestedLoopJoin(const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation) {
    std::vector<ResultRelation> resultTuples;
    for(int i = 0; i < leftRelation.size(); i += 1) {
        for(int j = 0; j < rightRelation.size(); j += 1) {
            if (leftRelation[i].movieId == rightRelation[j].titleId) {
                resultTuples.emplace_back(createResultTuple(leftRelation[i], rightRelation[j]));
            }
        }
    }
    return resultTuples;
}

TEST(NestedLoopTest, TestJoiningTuples) {
    std::cout << "Test reading data from a file.\n";

    const auto leftRelation = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_uniform_512mb.csv"));
    const auto rightRelation = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform_512mb.csv"));

    const auto resultTuples = performNestedLoopJoin(leftRelation, rightRelation);

    //assert(resultTuples.size() == 8457 && "Size of result tuples does not match expected size");
    ASSERT_EQ(resultTuples.size(), 8457);
    std::cout << "\n\n";
}
