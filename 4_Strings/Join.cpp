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
    // Handle empty string
    if (input.empty()) {
        return "";
    }

    std::ostringstream compressed;
    char lastChar = input[0];
    int count = 1;

    for (size_t i = 1; i < input.length(); ++i) {
        if (input[i] == lastChar) {
            ++count;
        } else {
            compressed << lastChar << count;
            lastChar = input[i];
            count = 1;
        }
    }
    // Append the last character and its count
    compressed << lastChar << count;

    return compressed.str();
}



std::vector<ResultRelation> performJoin(const std::vector<CastRelation>& castRelation, const std::vector<TitleRelation>& titleRelation, int numThreads) {
    std::cout << "Test: " << counterTest++ << std::endl;
    std::cout <<"castRelation.size(): " << castRelation.size() << std::endl;
    std::cout <<"titleRelation.size(): " << titleRelation.size() << std::endl;
    Trie<CastRelation> trie;
    std::vector<ResultRelation> results;
    results.reserve(200000);
    // Use numThreads threads to insert into Trie
    std::vector<std::jthread> threads;
    std::atomic_size_t counter = 0;
    for(int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&trie, &castRelation, &counter ] {
            while(counter < castRelation.size()) {
                auto localCounter = counter.fetch_add(1);
                auto compressed = compressString(castRelation[localCounter].note);
                std::string_view noteView(compressed);
                trie.insert(noteView, &castRelation[localCounter]);
            }
        });
    }
    for(auto& thread : threads) {
        thread.join();
    }
    std::vector<std::jthread> searchThreads;
    counter = 0;
    std::mutex m_results;
    for(int i = 0; i < numThreads; ++i) {
        searchThreads.emplace_back([&trie, &titleRelation, &results, &counter, &m_results] {
            std::vector<std::pair<const CastRelation*, const TitleRelation*>> localResults;
            while(counter < titleRelation.size()) {
                auto localCounter = counter.fetch_add(1);
                auto compressed = compressString(titleRelation[localCounter].title);
                std::string_view titleView(compressed);
                auto foundResults = trie.longestPrefix(titleView);
                if(!foundResults.empty()) {
                    for(const auto& result : foundResults) {
                        localResults.emplace_back(result, &titleRelation[localCounter]);
                    }
                }
            }
            std::scoped_lock l_results(m_results);
            std::cout << "localResults.size(): " << localResults.size() << std::endl;
            for(const auto&[castPtr, titlePtr]: localResults) {
                results.emplace_back(createResultTuple(*castPtr, *titlePtr));
            }
        });
    }
    for(auto& thread : searchThreads) {
        thread.join();
    }
    std::cout << "results.size(): " << results.size() << std::endl;
    return results;

    //---------------------------------------------------------------------------------------
    // TODO: Implement a join on the strings cast.note and title.title
    // The benchmark will join on increasing string sizes: cast.note% LIKE title.title
    //return performJointrie(castRelation, titleRelation, numThreads);
    //return {};
}
/*
int main(){
    const auto leftRelation = load<CastRelation>(DATA_DIRECTORY + std::string("cast_info_uniform.csv"), 20000);
    const auto rightRelation = load<TitleRelation>(DATA_DIRECTORY + std::string("title_info_uniform.csv"), 20000);
    auto result=  performJoin(leftRelation, rightRelation, 1);
    return 0;
}
 */

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
    const auto leftRelation = load<CastRelation>(DATA_DIRECTORY + std::string("cast_info_short_strings_200000.csv"));
    const auto rightRelation = load<TitleRelation>(DATA_DIRECTORY + std::string("title_info_short_strings_200000.csv"));
    Timer timer("Trie");
    timer.start();
    auto results = performJoin(leftRelation, rightRelation, 16);
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
    std::string input = "aaabbccccd";
    std::string compressed = compressString(input);
    std::cout << "Compressed string: " << compressed << std::endl;
}