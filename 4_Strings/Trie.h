#ifndef PPDS_PARALLELISM_TRIE_H
#define PPDS_PARALLELISM_TRIE_H

#include <mutex>
#include <string>
#include <optional>
#include <map>
#include <vector>
#include <iostream>

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
            if(node == nullptr) {
                std::cout << "Attempting insertion on null node!" << std::endl;
                std::cout << std::flush << std::endl;
                return; // Return if node is null (should not happen
            }
            std::lock_guard lock(node->m_dataVector); // Lock this node
            node->dataVector.emplace_back(ptr);
            return;
        }

        char currentChar = key[depth];
        std::unique_lock<std::mutex> lock(node->nodeMutex); // Lock this node

        if (node->children[currentChar] == nullptr || node->children.find(currentChar) == node->children.end()) {
            node->children[currentChar] = new TrieNode();
        }
        lock.unlock();
        TrieNode* nextNode = node->children[currentChar];
        insertRecursive(nextNode, key, depth + 1, ptr);
    }

    // Helper function to perform search recursively
    const std::vector<const T*>& searchRecursive(TrieNode* node, std::string_view key, size_t depth) {
        static std::vector<const T*> emptyVector;  // Static empty vector to return if no match found

        if (node == nullptr) return emptyVector;
        if (depth == key.length()) {
            return node->dataVector;
        }

        char currentChar = key[depth];
        std::lock_guard<std::mutex> lock(node->nodeMutex); // Lock this node

        if (node->children.find(currentChar) == node->children.end()) {
            return emptyVector;
        }

        return searchRecursive(node->children[currentChar], key, depth + 1);
    }

    // Helper function to perform longest prefix match recursively and return a reference to the vector of data pointers
    const std::vector<const T*>& longestPrefixRecursive(TrieNode* node, std::string_view key, size_t depth) {
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
    void insert(std::string_view key, const T* ptr) {
        if (key.empty()) return;

        TrieNode* currentNode = root;
        for (size_t depth = 0; depth < key.length(); ++depth) {
            char currentChar = key[depth];

            std::unique_lock<std::mutex> lock(currentNode->nodeMutex);
            if (currentNode->children.find(currentChar) == currentNode->children.end()) {
                currentNode->children[currentChar] = new TrieNode();
            }
            TrieNode* nextNode = currentNode->children[currentChar];
            lock.unlock();
            currentNode = nextNode;
        }

        std::lock_guard<std::mutex> lock(currentNode->m_dataVector);
        currentNode->dataVector.emplace_back(ptr);
    }

    // Search for an exact string_view in the Trie and return the first associated pointer
    const std::vector<const T*>& search(std::string_view key) {
        return searchRecursive(root, key, 0);
    }

    // Find the longest prefix match and return a reference to the vector of associated pointers
    const std::vector<const T*>& longestPrefix(std::string_view key) {
        return longestPrefixRecursive(root, key, 0);
    }
};

#endif // PPDS_PARALLELISM_TRIE_H
