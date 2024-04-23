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
//==------------------------- BENCHMARK TESTS --------------------------==//
//==--------------------------------------------------------------------==//

TEST(NestedLoopTest, TestParsingCastData) {
    std::cout << "Test reading cast data from a file.\n";

    const auto relation = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_uniform.csv"), 10);

    for(const auto& tuple : relation) {
        std::cout << castRelationToString(tuple) << '\n';
    }
}


TEST(NestedLoopTest, TestParsingTitleData) {
    std::cout << "Test reading data title from a file.\n";

    const auto relation = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform.csv"), 10);

    for(const auto& tuple : relation) {
        std::cout << titleRelationToString(tuple) << '\n';
    }
}


//==--------------------------------------------------------------------==//
//==------------------- TODO: JOIN IMPLEMENTATION ----------------------==//
//==--------------------------------------------------------------------==//

std::vector<ResultRelation> performNestedLoopJoin(const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation) {
    // TODO: Implement the nested loop join algorithm
    std::vector<ResultRelation> results;
    for (const auto left_entry: leftRelation) {
        for (const auto right_entry: rightRelation) {
            if (left_entry.movieId == right_entry.imdbId) {
                auto result = createResultTuple(left_entry, right_entry);
                results.emplace_back(result);
            }
        }
    }

    return results;
}

TEST(NestedLoopTest, TestJoiningTuples) {
    std::cout << "Test reading data from a file.\n";

    const auto leftRelation = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_uniform.csv"));
    const auto rightRelation = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform.csv"));

    const auto resultTuples = performNestedLoopJoin(leftRelation, rightRelation);

    std::cout << "\n\n#######################################\n";
    std::cout << "############### RESULTS ###############\n";
    std::cout << "#######################################\n";

    for(const auto& resultTuple : resultTuples) {
        std::cout << resultRelationToString(resultTuple) << '\n';
    }
    std::cout << "\n\n";
}
