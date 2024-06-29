#ifndef PPDS_PARALLELISM_TRIE_H
#include <atomic>
#include <memory>
#include <map>
#include <unordered_map>
#include "JoinUtils.hpp"

class Trie {
public:
    Trie();
    ~Trie();
    void insert(const std::string_view& key, const CastRelation* castRelation);
    const CastRelation* search(const std::string& key);
private:
    struct TrieNode {
        std::unordered_map<char, std::pair<std::mutex, TrieNode*>> children;
        const CastRelation* castRelation = nullptr;
    };
    TrieNode* root = nullptr;
};

#endif //PPDS_PARALLELISM_TRIE_H