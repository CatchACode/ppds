//
// Created by klaas on 28.06.24.
//
#include <mutex>
#include "Trie.h"


Trie::Trie() {
    root = new TrieNode();
}

Trie::~Trie() {
    delete root;
}

void Trie::insert(const std::string_view& key, const CastRelation* castRelation) {
    TrieNode* current = root;
    for(const char& c : key) {
        if(current->children[c].second == nullptr) {
            current->children[c].second = new TrieNode();
        }
        current = current->children[c].second;
    }
    current->castRelation = castRelation;
}