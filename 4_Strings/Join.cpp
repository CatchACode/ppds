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

std::vector<ResultRelation> performJoin(const std::vector<CastRelation>& castRelation, const std::vector<TitleRelation>& titleRelation, int numThreads) {
    std::cout << "Printing CastRelation notes!\n";
    for(const auto& record: castRelation) {
        std::cout << record.note << std::endl;
    }
    std::cout << "\n\nPrinting TitleRelation titles!\n";
    for(const auto& record: titleRelation) {
        std::cout << record.title << std::endl;
    }
    std::cout << "\n\n";
    Trie trie;
    std::vector<ResultRelation> results;
    // Use numThreads threads to insert into Trie
    std::vector<std::jthread> threads;
    std::atomic_size_t counter = 0;
    for(int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&trie, &titleRelation, &counter ] {
            while(counter < titleRelation.size()) {
                auto localCounter = counter.fetch_add(1);
                trie.insert(std::string_view(titleRelation[localCounter].title), &titleRelation[localCounter]);
            }
        });
    }
    for(auto& thread : threads) {
        thread.join();
    }
    std::vector<std::jthread> searchThreads;
    counter = 0;
    std::mutex m_results;
    /*
    for(int i = 0; i < numThreads; ++i) {
        searchThreads.emplace_back([&trie, &titleRelation, &results, &counter, &m_results] {
            while(counter < titleRelation.size()) {
                auto localCounter = counter.fetch_add(1);
                auto result = trie.search(std::string_view(titleRelation[localCounter].title, 200));
                if(result != nullptr) {
                    std::lock_guard lock(m_results);
                    results.emplace_back(createResultTuple(*result, titleRelation[localCounter]));
                }
            }
        });
    }
    for(auto& thread : searchThreads) {
        thread.join();
    }
     */
    for(const auto& title: titleRelation){
        auto result = trie.search(std::string_view(title.title));
        if(result != nullptr) {
            results.emplace_back(createResultTuple(*result, title));
        }
    }

    return results;

    //---------------------------------------------------------------------------------------
    // TODO: Implement a join on the strings cast.note and title.title
    // The benchmark will join on increasing string sizes: cast.note% LIKE title.title
    //return performJointrie(castRelation, titleRelation, numThreads);
    return {};
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
    const auto leftRelation = load<CastRelation>(DATA_DIRECTORY + std::string("cast_info_short_strings_20000.csv"), 20000);
    const auto rightRelation = load<TitleRelation>(DATA_DIRECTORY + std::string("title_info_short_strings_20000.csv"), 20000);
    Timer timer("Trie");
    timer.start();
    auto results = performJoin(leftRelation, rightRelation, 8);
    timer.pause();
    std::cout << "Join took: " << printString(timer) << std::endl;
    std::cout << results.size() << std::endl;
}