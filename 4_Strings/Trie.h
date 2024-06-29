#ifndef PPDS_PARALLELISM_TRIE_H
#include <mutex>
#include <string>
#include <optional>
#include <map>

#include "JoinUtils.hpp"
template<typename T>
class Trie {
private:
    struct TrieNode {
        std::map<char, TrieNode*> children;
        const T* dataPtr;
        std::mutex nodeMutex;  // Mutex for thread safety

        TrieNode() : dataPtr(nullptr) {}
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
    void insertRecursive(TrieNode* node, std::string_view key, size_t depth, const T* ptr) {
        if (depth == key.length()) {
            node->dataPtr = ptr;
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
    const T* searchRecursive(TrieNode* node, std::string_view key, size_t depth) {
        if (node == nullptr) return nullptr;
        if (depth == key.length()) return node->dataPtr;

        char currentChar = key[depth];
        std::lock_guard<std::mutex> lock(node->nodeMutex); // Lock this node

        if (node->children.find(currentChar) == node->children.end()) {
            return nullptr;
        }

        return searchRecursive(node->children[currentChar], key, depth + 1);
    }

    const T* longestPrefixMatch(TrieNode* node, std::string_view key, size_t depth, const T* longestMatchPtr) {
        if (node == nullptr) return longestMatchPtr;
        if (node->dataPtr != nullptr) {
            longestMatchPtr = node->dataPtr;
        }
        if (depth == key.length()) return longestMatchPtr;

        char currentChar = key[depth];
        std::lock_guard<std::mutex> lock(node->nodeMutex); // Lock this node

        if (node->children.find(currentChar) == node->children.end()) {
            return longestMatchPtr;
        }

        return longestPrefixMatch(node->children[currentChar], key, depth + 1, longestMatchPtr);
    }

public:
    Trie() {
        root = new TrieNode();
    }

    ~Trie() {
        recursiveDelete(root);
    }

    // Insert a string_view and corresponding pointer into the Trie
    void insert(std::string_view key, const T* ptr) {
        insertRecursive(root, key, 0, ptr);
    }

    // Search for a string_view in the Trie and return corresponding pointer
    const T* search(std::string_view key) {
        return searchRecursive(root, key, 0);
    }

    // Find the longest prefix match and return its dataPtr
    const T* longestPrefix(std::string_view key) {
        return longestPrefixMatch(root, key, 0, nullptr);
    }
};
#endif //PPDS_PARALLELISM_TRIE_H