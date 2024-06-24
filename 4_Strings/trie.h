//
// Created by boss on 17.06.24.
//

#ifndef PPDS_TRIE_H
#define PPDS_TRIE_H

#include<iostream>
#include<cstdio>
#include<string>
#include<vector>
#include<algorithm>
#include<cmath>
#include<map>

using namespace std;
struct TrieNode
{
    map<char,TrieNode*> children;
    bool endofword;
    int index;
    TrieNode()
    {
        endofword=false;
    }
};
struct Ruckgabe
{
    bool endofword;
    int index;
};

void insert(TrieNode *root,string word, int index)
{
    TrieNode *current=root;
    for(int i=0;i<word.size();i++)
    {
        char ch=word[i];
        TrieNode *node=current->children[ch];
        if(!node)
        {
            node = new TrieNode();
            current->children[word[i]]=node;
        }
        current=node;
    }
    current->endofword=true;
    current->index= index;
}
Ruckgabe search(TrieNode *root,string word)
{
    Ruckgabe ret;
    TrieNode *current=root;
    for(int i=0;i<word.size();i++)
    {
        char ch=word[i];
        TrieNode *node=current->children[ch];
        if(!node) {
            ret.endofword = false;
            ret.index = INT32_MAX;
            return ret;
        }
        current=node;
    }
    ret.endofword = current->endofword;
    ret.index = current->index;
    return ret;
}

void printTrie(TrieNode* root, string prefix = "") {
    if (root->endofword) {
        cout << "Das Wort steht am index " << root->index << endl;
        cout << prefix << endl;
    }
    for (auto it: root->children) {
        printTrie(it.second, prefix + it.first);
    }
}

vector<ResultRelation> performJointrie(const vector<CastRelation>& castRelation, const vector<TitleRelation>& titleRelation, int numThreads){
    vector<ResultRelation> resultTuples;
    // Aufbau des Trie auf der CastRelation
    TrieNode *root=new TrieNode();
    for(int i = 0; i < castRelation.size(); i++){
        insert(root,castRelation[i].note,i);
    }
    for(int j = 0; j < titleRelation.size(); j++){
        Ruckgabe gefunden = search(root, titleRelation[j].title);
        if(gefunden.endofword){
            resultTuples.emplace_back(createResultTuple(castRelation[gefunden.index], titleRelation[j]));
        }
    }
    return  resultTuples;
}

#endif //PPDS_TRIE_H
