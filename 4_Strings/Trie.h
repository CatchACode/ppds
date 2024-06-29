#ifndef PPDS_PARALLELISM_TRIE_H
#include <mutex>
#include <string>
#include <optional>
#include <map>

#include "JoinUtils.hpp"

class Trie {
private:
    struct TrieNode {
        std::map<char, TrieNode*> children;
        const CastRelation* castRelationPtr;
        std::mutex nodeMutex;  // Mutex for thread safety

        TrieNode() : castRelationPtr(nullptr) {}
    };
    TrieNode* root;

    // Helper function to recursively delete nodes
    void recursiveDelete(TrieNode* node) {
        if (node == nullptr) return;
        for (auto& child : node->children) {
            recursiveDelete(child.second);
        }
        delete node;
    }

    // Helper function to perform insertion recursively
    void insertRecursive(TrieNode* node, std::string_view key, size_t depth, const CastRelation* ptr) {
        if (depth == key.length()) {
            node->castRelationPtr = ptr;
            return;
        }

        char currentChar = key[depth];
        std::unique_lock<std::mutex> lock(node->nodeMutex); // Lock this node

        if (node->children.find(currentChar) == node->children.end()) {
            node->children[currentChar] = new TrieNode();
        }
        lock.unlock();
        insertRecursive(node->children[currentChar], key, depth + 1, ptr);
    }

    // Helper function to perform search recursively
    const CastRelation* searchRecursive(TrieNode* node, std::string_view key, size_t depth) {
        if (node == nullptr) return nullptr;
        if (depth == key.length()) return node->castRelationPtr;

        char currentChar = key[depth];
        std::lock_guard<std::mutex> lock(node->nodeMutex); // Lock this node

        if (node->children.find(currentChar) == node->children.end()) {
            return nullptr;
        }

        return searchRecursive(node->children[currentChar], key, depth + 1);
    }

public:
    Trie() {
        root = new TrieNode();
    }

    ~Trie() {
        recursiveDelete(root);
    }

    // Insert a string_view and corresponding pointer into the Trie
    void insert(std::string_view key, const CastRelation* ptr) {
        insertRecursive(root, key, 0, ptr);
    }

    // Search for a string_view in the Trie and return corresponding pointer
    const CastRelation* search(std::string_view key) {
        return searchRecursive(root, key, 0);
    }
};
#endif //PPDS_PARALLELISM_TRIE_H