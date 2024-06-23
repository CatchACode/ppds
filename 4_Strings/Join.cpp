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

//std::vector<ResultRelation> performJoin(const std::vector<CastRelation>& castRelation, const std::vector<TitleRelation>& titleRelation, int numThreads) {
void performJoin() {

    //omp_set_num_threads(numThreads);
    //std::vector<ResultRelation> resultTuples;
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
    bool found = search(root, wordToSearch);
    cout << "Das Wort '" << wordToSearch << "' ist im Präfixbaum enthalten: " << (found ? "Ja" : "Nein") << endl;

    printTrie(root);
    //---------------------------------------------------------------------------------------
    // TODO: Implement a join on the strings cast.note and title.title
    // The benchmark will join on increasing string sizes: cast.note% LIKE title.title

    //return resultTuples;
}

int main(){
    performJoin();
    return 0;
    return 0;
}
