#ifndef PPDS_PARALLELISM_TRIE_H
#define PPDS_PARALLELISM_TRIE_H

#include <mutex>
#include <string>
#include <optional>
#include <map>
#include <vector>

template<typename T>
class Trie {
private:
    struct TrieNode {
        std::map<char, TrieNode*> children;
        std::vector<const T*> dataVector;
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
    void insertRecursive(TrieNode* node, const std::string& key, size_t depth, const T* ptr) {
        if (depth == key.length()) {
            std::lock_guard lock(node->nodeMutex); // Lock this node
            node->dataVector.emplace_back(ptr);
            return;
        }

        char currentChar = key[depth];
        std::unique_lock<std::mutex> lock(node->nodeMutex); // Lock this node

        if (node->children[currentChar] == nullptr || node->children.find(currentChar) == node->children.end()) {
            node->children[currentChar] = new TrieNode();
        }
        lock.unlock();
        insertRecursive(node->children[currentChar], key, depth + 1, ptr);
    }

    // Helper function to perform search recursively
    const std::vector<const T*>& searchRecursive(TrieNode* node, const std::string& key, size_t depth) {
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
    const std::vector<const T*>& longestPrefixRecursive(TrieNode* node, const std::string& key, size_t depth) {
        static std::vector<const T*> emptyVector;  // Static empty vector to return if no match found

        if (node == nullptr) return emptyVector;
        if (depth >= key.length() || node->dataVector.size() > 0) {
            return node->dataVector;
        }

        char currentChar = key[depth];
        std::lock_guard<std::mutex> lock(node->nodeMutex); // Lock this node

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
    void insert(const std::string& key, const T* ptr) {
        if(key.empty()) return;
        insertRecursive(root, key, 0, ptr);
    }

    // Search for an exact string_view in the Trie and return the first associated pointer
    const std::vector<const T*>& search(const std::string& key) {
        return searchRecursive(root, key, 0);
    }

    // Find the longest prefix match and return a reference to the vector of associated pointers
    const std::vector<const T*>& longestPrefix(const std::string& key) {
        return longestPrefixRecursive(root, key, 0);
    }
};

#endif // PPDS_PARALLELISM_TRIE_H
