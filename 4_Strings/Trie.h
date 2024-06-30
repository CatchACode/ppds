#ifndef PPDS_PARALLELISM_TRIE_H
#define PPDS_PARALLELISM_TRIE_H

#include <mutex>
#include <string>
#include <optional>
#include <map>
#include <vector>
#include <iostream>
#include <shared_mutex>
#include <unordered_map>

template<typename T>
class Trie {
private:
    struct TrieNode {
        std::map<char, TrieNode*> children;
        std::vector<const T*> dataVector;
        std::mutex m_dataVector;  // Mutex for thread safety
        std::mutex nodeMutex;  // Mutex for thread safety

        TrieNode() {}
    };
    TrieNode* root;

    // Helper function to recursively delete nodes
    inline void recursiveDelete(TrieNode* node) {
        if (node == nullptr) return;
        for (auto& child : node->children) {
            recursiveDelete(child.second);
        }
        delete node;
    }

    // Helper function to perform longest prefix match recursively and return a reference to the vector of data pointers
    inline const std::vector<const T*>& longestPrefixRecursive(TrieNode* node, std::string_view key, size_t depth) {
        static std::vector<const T*> emptyVector;  // Static empty vector to return if no match found

        if (node == nullptr) return emptyVector;
        if (depth >= key.length() || node->dataVector.size() > 0) {
            return node->dataVector;
        }

        char currentChar = key[depth];

        if (node->children.find(currentChar) == node->children.end()) {
            return emptyVector;
        }

        return longestPrefixRecursive(node->children[currentChar], key, depth + 1);
    }

public:
    Trie() {
        root = new TrieNode();
    }

    ~Trie() {
        recursiveDelete(root);
    }

    // Insert a string_view and corresponding pointer into the Trie
    inline void insert(std::string_view key, const T* ptr) {
        if (key.empty()) return;

        TrieNode* currentNode = root;
        for (size_t depth = 0; depth < key.length(); ++depth) {
            char currentChar = key[depth];

            currentNode->nodeMutex.lock();
            if (currentNode->children.find(currentChar) == currentNode->children.end()) {
                currentNode->children[currentChar] = new TrieNode();
            }
            TrieNode* nextNode = currentNode->children[currentChar];
            currentNode->nodeMutex.unlock();
            currentNode = nextNode;
        }

        std::lock_guard<std::mutex> lock(currentNode->m_dataVector);
        currentNode->dataVector.emplace_back(ptr);
    }

    // Search for an exact string_view in the Trie iteratively
    inline const std::vector<const T*>& search(std::string_view key) {
        static std::vector<const T*> emptyVector;  // Static empty vector to return if no match found
        TrieNode* currentNode = root;

        for (size_t depth = 0; depth < key.length(); ++depth) {
            char currentChar = key[depth];

            if (currentNode->children.find(currentChar) == currentNode->children.end()) {
                return emptyVector;
            }
            currentNode = currentNode->children[currentChar];
        }

        return currentNode->dataVector;
    }

    // Find the longest prefix match and return a reference to the vector of associated pointers
    inline const std::vector<const T*>& longestPrefix(std::string_view key) {
        return longestPrefixRecursive(root, key, 0);
    }
};

#endif // PPDS_PARALLELISM_TRIE_H
