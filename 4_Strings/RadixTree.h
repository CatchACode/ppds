//
// Created by klaas on 30.06.24.
//

#ifndef PPDS_PARALLELISM_RADIX_TRIE_H
#define PPDS_PARALLELISM_RADIX_TRIE_H

#include <mutex>
#include <string>
#include <optional>
#include <map>
#include <vector>
#include <iostream>
#include <unordered_map>
#include <shared_mutex>

template<typename T>
class RadixTrie {
private:
    struct TrieNode {
        std::unordered_map<std::string, TrieNode*> children;
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

        for (const auto& [childKey, childNode] : node->children) {
            if (key.substr(depth, childKey.size()) == childKey) {
                return longestPrefixRecursive(childNode, key, depth + childKey.size());
            }
        }

        return emptyVector;
    }

public:
    RadixTrie() {
        root = new TrieNode();
    }

    ~RadixTrie() {
        recursiveDelete(root);
    }

    // Insert a string_view and corresponding pointer into the RadixTrie
    inline void insert(std::string_view key, const T* ptr) {
        if (key.empty()) return;

        TrieNode* currentNode = root;
        size_t depth = 0;

        while (depth < key.length()) {
            currentNode->nodeMutex.lock();
            bool found = false;

            for (auto& [childKey, childNode] : currentNode->children) {
                size_t matchLength = 0;
                while (matchLength < childKey.length() && depth + matchLength < key.length() && childKey[matchLength] == key[depth + matchLength]) {
                    ++matchLength;
                }

                if (matchLength == childKey.length()) {
                    currentNode->nodeMutex.unlock();
                    currentNode = childNode;
                    depth += matchLength;
                    found = true;
                    break;
                } else if (matchLength > 0) {
                    TrieNode* newChild = new TrieNode();
                    TrieNode* existingChild = currentNode->children[childKey];
                    std::string newChildKey = childKey.substr(0, matchLength);
                    std::string remainingChildKey = childKey.substr(matchLength);

                    newChild->children[remainingChildKey] = existingChild;
                    currentNode->children.erase(childKey);
                    currentNode->children[newChildKey] = newChild;

                    currentNode->nodeMutex.unlock();
                    currentNode = newChild;
                    depth += matchLength;
                    found = true;
                    break;
                }
            }

            if (!found) {
                std::string newKey = std::string(key.substr(depth));
                currentNode->children[newKey] = new TrieNode();
                currentNode->nodeMutex.unlock();
                currentNode = currentNode->children[newKey];
                break;
            }
        }

        std::lock_guard<std::mutex> lock(currentNode->m_dataVector);
        currentNode->dataVector.emplace_back(ptr);
    }

    // Search for an exact string_view in the RadixTrie iteratively
    inline const std::vector<const T*>& search(std::string_view key) {
        static std::vector<const T*> emptyVector;  // Static empty vector to return if no match found
        TrieNode* currentNode = root;
        size_t depth = 0;

        while (depth < key.length()) {
            bool found = false;

            for (const auto& [childKey, childNode] : currentNode->children) {
                if (key.substr(depth, childKey.length()) == childKey) {
                    currentNode = childNode;
                    depth += childKey.length();
                    found = true;
                    break;
                }
            }

            if (!found) {
                return emptyVector;
            }
        }

        return currentNode->dataVector;
    }

    // Find the longest prefix match and return a reference to the vector of associated pointers
    inline const std::vector<const T*>& longestPrefix(std::string_view key) {
        return longestPrefixRecursive(root, key, 0);
    }
};

#endif // PPDS_PARALLELISM_RADIX_TRIE_H