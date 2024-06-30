//
// Created by klaas on 30.06.24.
//

#ifndef PPDS_4_STRINGS_PATHCOMPRESSIONTRIE_H
#define PPDS_4_STRINGS_PATHCOMPRESSIONTRIE_H

#include <mutex>
#include <string>
#include <optional>
#include <map>
#include <vector>
#include <iostream>
#include <memory>

template<typename T>
class PathCompressionTrie {
private:
    struct TrieNode {
        std::map<char, std::unique_ptr<TrieNode>> children;
        std::vector<const T*> dataVector;
        std::string edgeLabel; // Label on the edge to this node
        std::mutex m_dataVector;  // Mutex for thread safety
        std::mutex nodeMutex;  // Mutex for thread safety

        TrieNode() : edgeLabel("") {}
    };

    std::unique_ptr<TrieNode> root;

    // Helper function to recursively delete nodes
    void recursiveDelete(TrieNode* node) {
        if (node == nullptr) return;
        for (auto& child : node->children) {
            recursiveDelete(child.second.get());
        }
        delete node;
    }

public:
    PathCompressionTrie() {
        root = std::make_unique<TrieNode>();
    }

    ~PathCompressionTrie() {
        recursiveDelete(root.get());
    }

    // Insert a string_view and corresponding pointer into the Trie iteratively
    void insert(std::string_view key, const T* ptr) {
        if (key.empty()) return;

        TrieNode* currentNode = root.get();
        size_t depth = 0;

        while (depth < key.length()) {
            std::unique_lock<std::mutex> lock(currentNode->nodeMutex);
            char currentChar = key[depth];

            // If no matching child, create one
            if (currentNode->children.find(currentChar) == currentNode->children.end()) {
                auto newNode = std::make_unique<TrieNode>();
                newNode->edgeLabel = key.substr(depth);
                currentNode->children[currentChar] = std::move(newNode);
                currentNode = currentNode->children[currentChar].get();
                lock.unlock();
                break;
            }

            TrieNode* childNode = currentNode->children[currentChar].get();
            lock.unlock();

            // Find the longest common prefix between the edge label and the remaining key
            std::string_view edgeLabel = childNode->edgeLabel;
            size_t matchLength = 0;
            while (matchLength < edgeLabel.length() && depth + matchLength < key.length() &&
                   edgeLabel[matchLength] == key[depth + matchLength]) {
                matchLength++;
            }

            if (matchLength == edgeLabel.length()) {
                // Full match of the edge label, move to the next node
                currentNode = childNode;
                depth += matchLength;
            } else {
                // Split the edge
                auto newChildNode = std::make_unique<TrieNode>();
                newChildNode->edgeLabel = edgeLabel.substr(matchLength);
                newChildNode->children = std::move(childNode->children);
                newChildNode->dataVector = std::move(childNode->dataVector);

                auto splitNode = std::make_unique<TrieNode>();
                splitNode->edgeLabel = edgeLabel.substr(0, matchLength);
                splitNode->children[edgeLabel[matchLength]] = std::move(newChildNode);

                childNode->edgeLabel = key.substr(depth + matchLength);
                splitNode->children[key[depth + matchLength]] = std::move(currentNode->children[currentChar]);

                currentNode->children[currentChar] = std::move(splitNode);
                currentNode = currentNode->children[currentChar].get()->children[key[depth + matchLength]].get();
                depth = key.length(); // End the loop
            }
        }

        std::lock_guard<std::mutex> lock(currentNode->m_dataVector);
        currentNode->dataVector.emplace_back(ptr);
    }

    // Search for an exact string_view in the Trie iteratively
    const std::vector<const T*>& search(std::string_view key) {
        static std::vector<const T*> emptyVector;  // Static empty vector to return if no match found
        TrieNode* currentNode = root.get();
        size_t depth = 0;

        while (depth < key.length()) {
            std::lock_guard<std::mutex> lock(currentNode->nodeMutex);
            char currentChar = key[depth];

            if (currentNode->children.find(currentChar) == currentNode->children.end()) {
                return emptyVector;
            }

            TrieNode* childNode = currentNode->children[currentChar].get();
            std::string_view edgeLabel = childNode->edgeLabel;
            size_t matchLength = 0;

            while (matchLength < edgeLabel.length() && depth + matchLength < key.length() &&
                   edgeLabel[matchLength] == key[depth + matchLength]) {
                matchLength++;
            }

            if (matchLength != edgeLabel.length() || depth + matchLength > key.length()) {
                return emptyVector;
            }

            currentNode = childNode;
            depth += matchLength;
        }

        return currentNode->dataVector;
    }

    // Find the longest prefix match iteratively and return a reference to the vector of associated pointers
    const std::vector<const T*>& longestPrefix(std::string_view key) {
        static std::vector<const T*> emptyVector;  // Static empty vector to return if no match found
        TrieNode* currentNode = root.get();
        std::vector<const T*>* resultVector = &emptyVector;
        size_t depth = 0;

        while (depth < key.length()) {
            std::lock_guard<std::mutex> lock(currentNode->nodeMutex);
            char currentChar = key[depth];

            if (currentNode->children.find(currentChar) == currentNode->children.end()) {
                break; // No further match
            }

            TrieNode* childNode = currentNode->children[currentChar].get();
            std::string_view edgeLabel = childNode->edgeLabel;
            size_t matchLength = 0;

            while (matchLength < edgeLabel.length() && depth + matchLength < key.length() &&
                   edgeLabel[matchLength] == key[depth + matchLength]) {
                matchLength++;
            }

            if (matchLength == 0) {
                break;
            }

            currentNode = childNode;
            depth += matchLength;

            if (!currentNode->dataVector.empty()) {
                resultVector = &currentNode->dataVector;
            }
        }

        return *resultVector;
    }
};

#endif //PPDS_4_STRINGS_PATHCOMPRESSIONTRIE_H
