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
#include "NestedLoopJoin.h"
#include "SortMergeJoin.h"

std::vector<ResultRelation> performJoin(const std::vector<CastRelation>& castRelation, const std::vector<TitleRelation>& titleRelation, int numThreads) {
    return performThreadedSortJoin(castRelation, titleRelation, numThreads);
}


TEST(MemoryHierarchyTest, TestJoiningTuples) {
    const auto leftRelation = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_uniform.csv"));
    const auto rightRelation = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform.csv"));
    auto results = performJoin(leftRelation, rightRelation, 8);

    std::cout << "Result size: " << results.size() << std::endl;
    std::cout << "\n\n";
}

/**
 * Tests if the join algorithmus returns the correct amount of Tuples, and that the individual tuples are part of a correct
 * join algorithm, using uniform data!
 */
TEST(ParallelizationTest, TestAmoutUniform) {
    std::cout   << "##########################################################################################\n"
                << "# Testing if join algorithm contains wrong or duplicate records when using uniform data! #\n"
                << "##########################################################################################\n";
    const auto leftRelation = load<CastRelation>(DATA_DIRECTORY + std::string("cast_info_uniform.csv"), 20000);
    const auto rightRelation = load<TitleRelation>(DATA_DIRECTORY + std::string("title_info_uniform.csv"), 20000);

    size_t correctRecords = 0;
    size_t duplicateRecords = 0;
    size_t wrongRecords = 0;

    std::vector<uint32_t> duplicateCastInfoIds;
    std::vector<uint32_t> wrongCastInfoIds;

    auto correctResults = performNestedLoopJoin(leftRelation, rightRelation);
    std::unordered_map<uint32_t, const ResultRelation*> mapCorrect;
    std::unordered_map<uint32_t, const ResultRelation*> found; // Used to make sure no record is duplicated
    for(const auto& record : correctResults) {
        mapCorrect[record.castInfoId] = &record;
    }
    auto sample = performJoin(leftRelation, rightRelation, 8);

    for(const auto& record: sample) {
        if(!mapCorrect.contains(record.castInfoId)) {
            std::cout << "Found a record that should not be in results!\n" << resultRelationToString(record) << '\n' << std::endl;
            wrongRecords += 1;
            wrongCastInfoIds.emplace_back(record.castInfoId);
            if(found.contains(record.castInfoId)) {
                std::cout << "Found a duplicate record!\n" << resultRelationToString(record) << '\n' << std::endl;
                duplicateRecords += 1;
            } else {
                found[record.castInfoId] = &record;
            }
        }
        else {
            if(found.contains(record.castInfoId)) {
                std::cout << "Found a duplicate record!\n" << resultRelationToString(record) << '\n' << std::endl;
                duplicateRecords += 1;
            } else {
                found[record.castInfoId] = &record;
            }
            correctRecords += 1;
        }
    }
    std::cout << '\n';
    std::cout << "Found " << correctRecords << " correct records!\n";
    std::cout << "Found " << duplicateRecords << " duplicate records with the following CastInfoIds:\n";
    for(const auto& id: duplicateCastInfoIds) {
        std::cout << id << '\n';
    }
    std::cout << "Found " << wrongRecords << " wrong records with teh following CastInfoIds:\n";
    for(const auto& id: wrongCastInfoIds) {
        std::cout << id << '\n';
    }
    std::cout << std::flush;
}


/**
 * Tests if the join algorithmus returns the correct amount of Tuples, and that the individual tuples are part of a correct
 * join algorithm, using a dataset where each title record matches to one cast record!
 */
TEST(ParallelizationTest, TestAmoutOnlyMatches1to1) {
    std::cout   << "##########################################################################################\n"
                << "# Testing if join algorithm contains wrong or duplicate records when using uniform data! #\n"
                << "##########################################################################################\n";
    const auto leftRelation = load<CastRelation>(DATA_DIRECTORY + std::string("cast_info_matching.csv"), 20000);
    const auto rightRelation = load<TitleRelation>(DATA_DIRECTORY + std::string("title_info_matching.csv"), 20000);

    size_t correctRecords = 0;
    size_t duplicateRecords = 0;
    size_t wrongRecords = 0;

    std::vector<uint32_t> duplicateCastInfoIds;
    std::vector<uint32_t> wrongCastInfoIds;

    auto correctResults = performNestedLoopJoin(leftRelation, rightRelation);
    std::unordered_map<uint32_t, const ResultRelation*> mapCorrect;
    std::unordered_map<uint32_t, const ResultRelation*> found; // Used to make sure no record is duplicated
    for(const auto& record : correctResults) {
        mapCorrect[record.castInfoId] = &record;
    }
    auto sample = performJoin(leftRelation, rightRelation, 8);

    for(const auto& record: sample) {
        if(!mapCorrect.contains(record.castInfoId)) {
            std::cout << "Found a record that should not be in results!\n" << resultRelationToString(record) << '\n' << std::endl;
            wrongRecords += 1;
            wrongCastInfoIds.emplace_back(record.castInfoId);
            if(found.contains(record.castInfoId)) {
                std::cout << "Found a duplicate record!\n" << resultRelationToString(record) << '\n' << std::endl;
                duplicateRecords += 1;
            } else {
                found[record.castInfoId] = &record;
            }
        }
        else {
            if(found.contains(record.castInfoId)) {
                std::cout << "Found a duplicate record!\n" << resultRelationToString(record) << '\n' << std::endl;
                duplicateRecords += 1;
            } else {
                found[record.castInfoId] = &record;
            }
            correctRecords += 1;
        }
    }
    std::cout << '\n';
    std::cout << "Found " << correctRecords << " correct records!\n";
    std::cout << "Found " << duplicateRecords << " duplicate records with the following CastInfoIds:\n";
    for(const auto& id: duplicateCastInfoIds) {
        std::cout << id << '\n';
    }
    std::cout << "Found " << wrongRecords << " wrong records with teh following CastInfoIds:\n";
    for(const auto& id: wrongCastInfoIds) {
        std::cout << id << '\n';
    }
    std::cout << std::flush;
}

/**
 * Tests if the join algorithmus returns the correct amount of Tuples, and that the individual tuples are part of a correct
 * join algorithm!
 */
TEST(ParallelizationTest, TestAmoutOnlyMatches1toMany) {
    std::cout   << "##########################################################################################\n"
                << "# Testing if join algorithm contains wrong or duplicate records when using uniform data! #\n"
                << "##########################################################################################\n";
    const auto leftRelation = load<CastRelation>(DATA_DIRECTORY + std::string("cast_info_one_to_many.csv"), 20000);
    const auto rightRelation = load<TitleRelation>(DATA_DIRECTORY + std::string("title_info_one_to_many.csv"), 20000);

    size_t correctRecords = 0;
    size_t duplicateRecords = 0;
    size_t wrongRecords = 0;

    std::vector<uint32_t> duplicateCastInfoIds;
    std::vector<uint32_t> wrongCastInfoIds;

    auto correctResults = performNestedLoopJoin(leftRelation, rightRelation);
    std::unordered_map<uint32_t, const ResultRelation*> mapCorrect;
    std::unordered_map<uint32_t, const ResultRelation*> found; // Used to make sure no record is duplicated
    for(const auto& record : correctResults) {
        mapCorrect[record.castInfoId] = &record;
    }
    auto sample = performJoin(leftRelation, rightRelation, 8);

    for(const auto& record: sample) {
        if(!mapCorrect.contains(record.castInfoId)) {
            std::cout << "Found a record that should not be in results!\n" << resultRelationToString(record) << '\n' << std::endl;
            wrongRecords += 1;
            wrongCastInfoIds.emplace_back(record.castInfoId);
            if(found.contains(record.castInfoId)) {
                std::cout << "Found a duplicate record!\n" << resultRelationToString(record) << '\n' << std::endl;
                duplicateRecords += 1;
            } else {
                found[record.castInfoId] = &record;
            }
        }
        else {
            if(found.contains(record.castInfoId)) {
                std::cout << "Found a duplicate record!\n" << resultRelationToString(record) << '\n' << std::endl;
                duplicateRecords += 1;
            } else {
                found[record.castInfoId] = &record;
            }
            correctRecords += 1;
        }
    }
    std::cout << '\n';
    std::cout << "Found " << correctRecords << " correct records!\n";
    std::cout << "Found " << duplicateRecords << " duplicate records with the following CastInfoIds:\n";
    for(const auto& id: duplicateCastInfoIds) {
        std::cout << id << '\n';
    }
    std::cout << "Found " << wrongRecords << " wrong records with teh following CastInfoIds:\n";
    for(const auto& id: wrongCastInfoIds) {
        std::cout << id << '\n';
    }
    std::cout << std::flush;
}





