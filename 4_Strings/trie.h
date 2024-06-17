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
    TrieNode()
    {
        endofword=false;
    }
};
void insert(TrieNode *root,string word)
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
}
bool search(TrieNode *root,string word)
{
    TrieNode *current=root;
    for(int i=0;i<word.size();i++)
    {
        char ch=word[i];
        TrieNode *node=current->children[ch];
        if(!node)
            return false;
        current=node;
    }
    return current->endofword;
}

void printTrie(TrieNode* root, string prefix = "") {
    if (root->endofword) {
        cout << prefix << endl;
    }
    for (auto it: root->children) {
        printTrie(it.second, prefix + it.first);
    }
}


#endif //PPDS_TRIE_H
