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
#include "trie.h"
#include <unordered_map>
#include <iostream>
#include <gtest/gtest.h>
#include <omp.h>

std::vector<ResultRelation> performJoin(const std::vector<CastRelation>& castRelation, const std::vector<TitleRelation>& titleRelation, int numThreads) {
//void performJoin() {

    //omp_set_num_threads(numThreads);
    //std::vector<ResultRelation> resultTuples;
    /*
    //------------------------- Trie Test ----------------------------------
    vector<string> strs = {"apple", "banana", "orange", "apricot", "app"};
    for (int i = 0; i < strs.size(); i++) {
        cout << "Der Eintrag mit Index " << i << " lautet: " << strs[i] << endl;
    }
    TrieNode *root=new TrieNode();
    for (int i = 0; i < strs.size(); i++) {
        insert(root,strs[i],i);
    }
    string wordToSearch = "app";
    Ruckgabe found = search(root, wordToSearch);
    if(found.endofword){
        cout << "Das Wort '" << wordToSearch << "' ist im Präfixbaum enthalten und hat den index: " << found.index << endl;
    } else{
    cout << "Das Wort '" << wordToSearch << "' ist im Präfixbaum nicht enthalte." << endl;
    }

    cout << "----------------------------------------" << endl;
    printTrie(root);
     */
    //---------------------------------------------------------------------------------------
    // TODO: Implement a join on the strings cast.note and title.title
    // The benchmark will join on increasing string sizes: cast.note% LIKE title.title
    return performJointrie(castRelation, titleRelation, numThreads);
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
    const auto leftRelation = load<CastRelation>(DATA_DIRECTORY + std::string("cast_info_1StringMatch.csv"), 20000);
    const auto rightRelation = load<TitleRelation>(DATA_DIRECTORY + std::string("title_info_1StringMatch.csv"), 20000);

    auto results = performJoin(leftRelation, rightRelation, 8);

    std::cout << results.size() << std::endl;
}