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
#include "Trie.h"
#include <unordered_map>
#include <thread>
#include <iostream>
#include <gtest/gtest.h>
#include <string_view>
#include <omp.h>

static int counterTest = 0;

std::string compressString(const std::string &input) {
    if (input.empty()) return "";
    if(input[0] != '1') return input;

    std::string compressed;
    compressed.reserve(input.size()); // Reserve space to avoid multiple allocations

    char currentChar = input[0];
    int count = 1;

    for (size_t i = 1; i < input.size(); ++i) {
        if (input[i] == currentChar) {
            ++count;
        } else {
            compressed += currentChar;
            if (count > 1) {
                compressed += '*';
                compressed += std::to_string(count);
            }
            currentChar = input[i];
            count = 1;
        }
    }

    // Append the last character and its count
    compressed += currentChar;
    if (count > 1) {
        compressed += '*';
        compressed += std::to_string(count);
    }

    return compressed;
}



std::vector<ResultRelation> performJoin(const std::vector<CastRelation>& castRelation, const std::vector<TitleRelation>& titleRelation, int numThreads) {
    Trie<CastRelation> trie;
    std::vector<ResultRelation> results;
    results.resize(castRelation.size());
    #pragma omp parallel for num_threads(numThreads)
    for(const auto& castTuple: castRelation) {
        trie.insert(compressString(castTuple.note), &castTuple);
    }
    std::atomic_size_t resultIndex = 0;
    // OMP for loop to search for longest prefix
    #pragma omp parallel for num_threads(numThreads)
    for(const auto& titleTuple: titleRelation) {
        auto foundResults = trie.longestPrefix(compressString(titleTuple.title));
        if(!foundResults.empty()) {
            for(const auto& result : foundResults) {
                results[resultIndex++] = createResultTuple(*result, titleTuple);
            }
        }
    }
    results.resize(resultIndex);
    return results;
}

TEST(StringTest, TestNestedLoopjoin) {
    const auto leftRelation = load<CastRelation>(DATA_DIRECTORY + std::string("cast_info_uniform.csv"), 20000);
    const auto rightRelation = load<TitleRelation>(DATA_DIRECTORY + std::string("title_info_uniform.csv"), 20000);
    std::vector<ResultRelation> results;

    for(const auto& l_record: leftRelation) {
        for(const auto& r_record: rightRelation) {
            if(strncmp(l_record.note, r_record.title, 100) == 0) {
                results.emplace_back(createResultTuple(l_record, r_record));
            }
        }
    }
    std::cout << "results.size(): " << results.size() << std::endl;
}

TEST(StringTest, TestTrieJoin) {
    const auto leftRelation = load<CastRelation>(DATA_DIRECTORY + std::string("cast_info_long_strings_200000.csv"));
    const auto rightRelation = load<TitleRelation>(DATA_DIRECTORY + std::string("title_info_long_strings_200000.csv"));
    Timer timer("Trie");
    timer.start();
    auto results = performJoin(leftRelation, rightRelation, 8);
    timer.pause();
    std::cout << "Join took: " << printString(timer) << std::endl;
    std::cout << results.size() << std::endl;
}



TEST(StringTest, TestTrieJoinSingle) {
    const auto leftRelation = load<CastRelation>(DATA_DIRECTORY + std::string("cast_info_stolen_strings.csv"), 10);
    const auto rightRelation = load<TitleRelation>(DATA_DIRECTORY + std::string("title_info_short_strings_30.csv"), 30);
    Trie<CastRelation> trie;
    std::vector<ResultRelation> results;
    for(const auto& record: leftRelation) {
        trie.insert(record.note, &record);
    }
    for(const auto& record: rightRelation) {
        auto foundResults = trie.longestPrefix(record.title);
        for(const auto& result : foundResults) {
            results.emplace_back(createResultTuple(*result, record));
        }
    }
    std::cout << "results.size(): " << results.size() << std::endl;
}


TEST(StringTest, Bullshit) {
    std::vector<int32_t> test;
    test.reserve(128);
    std::vector<std::thread> threads;
    std::atomic_size_t counter = 0;
    for(int i = 0; i < std::thread::hardware_concurrency(); ++i) {
        threads.emplace_back([&test, &counter ] {
            while(counter < 128) {
                auto localCounter = counter.fetch_add(1);
                test.emplace(test.begin() + localCounter, localCounter);
            }
        });
    }
}

