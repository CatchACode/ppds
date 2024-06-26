//
// Created by CatchACode on 24.06.24.
//

#ifndef PPDS_3_PARTITIONING_PARTITIONING_H
#define PPDS_3_PARTITIONING_PARTITIONING_H

#include <array>
#include <vector>
#include <thread>
#include <cmath>
#include "JoinUtils.hpp"
#include "generated_variables.h"
#include <span>
#include "ThreadPool.h"
#include <map>

using CastIterator = std::vector<CastRelation>::iterator;
using TitleIterator = std::vector<TitleRelation>::iterator;

struct MapType {
    enum Type {CastRelationType, TitleRelationType} type;
    union {
        std::span<CastRelation> castRelationValue;
        std::span<TitleRelation> titleRelationValue;
    };

    MapType(std::span<CastRelation> span) : type(CastRelationType), castRelationValue(span) {}
    MapType(std::span<TitleRelation> span) : type(TitleRelationType), titleRelationValue(span) {}
    MapType(const CastIterator& begin, const CastIterator& end) : type(CastRelationType), castRelationValue(std::span(begin, end)) {}
    MapType(const TitleIterator& begin, const TitleIterator& end) : type(TitleRelationType), titleRelationValue(std::span(begin, end)) {}
};


static std::atomic_bool titlePartitioned(false);
static std::atomic_bool castPartitioned(false);
static std::size_t maxBitsToCompare;
static std::size_t numPartitionsToExpect;

static std::vector<ResultRelation> results;
static std::mutex m_results;
static std::vector<std::vector<ResultRelation>> resultsVector;
static std::mutex m_resultsVector;

constexpr static const std::size_t PARTITION_SIZE = L2_CACHE_SIZE / (sizeof(CastRelation*) + sizeof(uint32_t));

/**
 * returns the bit in the number at pos
 */

inline bool getBitAtPosition(int32_t num, int pos) {
    // Create a mask by left-shifting 1 by pos positions
    int mask = 1 << pos;
    // Perform bitwise AND with the mask and shift right to get the bit at pos
    return (num & mask) >> pos;
}

/**
 * Appends a bit (0,1) to the front of the steps number
 */
inline size_t appendStep(const uint8_t& steps, const bool step, const uint8_t position) {
    return steps + (step << position);
}

constexpr const std::size_t MAX_HASHMAP_SIZE = L2_CACHE_SIZE / (sizeof(int32_t) + sizeof(CastRelation*));

inline void setMaxBitsToCompare(const std::size_t sizeRelationVector) {
    auto minimumNumOfHashMaps = std::ceil(sizeRelationVector / MAX_HASHMAP_SIZE);
    if(minimumNumOfHashMaps == 0) {minimumNumOfHashMaps = 1;}
    maxBitsToCompare = static_cast<std::size_t>(std::ceil(std::log2(minimumNumOfHashMaps)));
    if(maxBitsToCompare == 0) {maxBitsToCompare = 1;}
    numPartitionsToExpect = static_cast<std::size_t>(std::pow(2, maxBitsToCompare));
}

inline uint32_t bitmask() {
    uint32_t mask = 0;
    for(int i = 0; i < maxBitsToCompare; ++i) {
        mask += 1 << i;
    }
    return mask;
}

void mergeResultVector(std::vector<ResultRelation> resultsApppend) {
    std::lock_guard lk2 (m_results);
    for(const auto& record: resultsApppend) {
        results.emplace_back(record);
    }

}



void hashJoin(ThreadPool& threadPool, std::span<CastRelation> leftRelation, std::span<TitleRelation> rightRelation) {
    if(leftRelation.size() == 0 || rightRelation.size() == 0) { // either span is empty so no matches
        return;
    }
    std::vector<ResultRelation> localResults;
    std::unordered_map<int32_t, const TitleRelation*> map;
    map.reserve(leftRelation.size());
    for(const auto& record: rightRelation) {
        map[record.titleId] = &record;
    }
    for(const auto& record: leftRelation) {
        if(map.contains(record.movieId)) {
            localResults.emplace_back(createResultTuple(record, *map[record.movieId]));
        }
    }
    threadPool.enqueue(mergeResultVector, std::move(localResults));
}




/**
 * @param begin iterator to the start of the cast relation
 * @param begin iterator to one past the last element of the cast relation, ie end();
 * @param position the bit position to compare
 * @return an iterator to the first element of the right partition
 *
 */
inline std::vector<CastRelation>::iterator castRadixPartition(const CastIterator& begin, const CastIterator& end, const uint8_t& position) {
    auto zeroBin = begin;
    auto oneBin = end;
    while(zeroBin != oneBin && zeroBin != end) {
        if(getBitAtPosition(zeroBin->movieId, position)) { // if true 1 is at the bit position
            std::swap(*zeroBin, *(--oneBin));
        } else { // a zero is at bit position
            zeroBin++;
        }
    }
    return zeroBin;
}

/**
 * @param begin iterator to the start of the title relation
 * @param begin iterator to one past the last element of the title relation, ie end();
 * @param position the bit position to compare
 * @return an iterator to the first element of the right partition
 *
 */
inline std::vector<TitleRelation>::iterator titleRadixPartition(const TitleIterator begin, const TitleIterator end, const uint8_t& position) {
    auto zeroBin = begin;
    auto oneBin = end;
    while(zeroBin != oneBin) {
        if(getBitAtPosition(zeroBin->titleId, position)) {
            std::swap(*zeroBin, *(--oneBin)); // because we conveniently want to use end();
        } else {
            zeroBin++;
        }
    }
    return zeroBin;
}

/** Recursively partitions a chunk using radix partitioning
 *
 * @param threadPool shared threadpool between all jobs
 * @param begin of chunk to partition
 * @param end of chunk (one past last element) of chunk to partition
 * @param m_titlePartitions mutex to ensure only one thread writes to the @param titlePartitions
 * @param titlePartitions vector with preallocated and initialized space for all possible partitions
 * @param counter counts number of finished partitions, so main thread can know when all final partition are created
 */

void inline titlePartition(ThreadPool& threadPool, const TitleIterator begin, const TitleIterator end,
                          uint8_t position, std::mutex& m_Partitions, std::unordered_multimap<int32_t, MapType>& partitions,
                          std::atomic_size_t& counter) {
    if(position >= maxBitsToCompare) {
        if(begin != end) {
            std::unique_lock lk(m_Partitions);
            if(partitions.count(begin->titleId & bitmask()) >= 1) {
                auto otherSpan = partitions.find(begin->titleId & bitmask());
                if(otherSpan->second.type == MapType::TitleRelationType) {
                    std::cerr << "Found a TitleRelation span with our bitmask!?";
                    throw std::runtime_error("Found a Titlerelation span with our bitmask?!");
                }
                auto span = otherSpan->second.castRelationValue;
                threadPool.enqueue(hashJoin, std::ref(threadPool), span, std::span(begin, end));
                partitions.erase(begin->titleId & bitmask()); // Delete it so we can check if any didn't match
            } else {
                partitions.insert({begin->titleId & bitmask(), MapType(begin, end)});
            }
        }
        counter++;
        counter.notify_all();
        return;
    }
    auto split = titleRadixPartition(begin, end, position);
    threadPool.enqueue(titlePartition, std::ref(threadPool), begin, split, position + 1, std::ref(m_Partitions), std::ref(partitions), std::ref(counter));
    threadPool.enqueue(titlePartition, std::ref(threadPool), split, end, position + 1, std::ref(m_Partitions), std::ref(partitions), std::ref(counter));
}

void inline castPartition(ThreadPool& threadPool, CastIterator begin, CastIterator end,
                          uint8_t position, std::mutex& m_Partitions, std::unordered_multimap<int32_t, MapType>& partitions,
                          std::atomic_size_t& counter) {
    if(position >= maxBitsToCompare) {
        if(begin != end) {
            std::unique_lock lk(m_Partitions);
            if(partitions.count(begin->movieId & bitmask()) >= 1) {
                auto otherSpan = partitions.find(begin->movieId & bitmask());
                if(otherSpan->second.type == MapType::CastRelationType) {
                    std::cerr << "Found a CastRelation span with our bitmask!?";
                    throw std::runtime_error("Found a CastRelation span with our bitmask?!");
                }
                auto span = otherSpan->second.titleRelationValue;
                threadPool.enqueue(hashJoin, std::ref(threadPool), std::span(begin, end), span);
                partitions.erase(begin->movieId & bitmask()); // Delete it so we can check if any didn't match
            } else {
                partitions.insert({begin->movieId & bitmask(), MapType(begin, end)});
            }
        }
        counter++;
        counter.notify_all();
        return;
    }
    auto split = castRadixPartition(begin, end, position);
    threadPool.enqueue(castPartition, std::ref(threadPool), begin, split, position + 1, std::ref(m_Partitions), std::ref(partitions), std::ref(counter));
    threadPool.enqueue(castPartition, std::ref(threadPool), split, end, position + 1, std::ref(m_Partitions), std::ref(partitions), std::ref(counter));
}

std::vector<ResultRelation> performPartitionJoin(const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation, unsigned int numThreads = std::jthread::hardware_concurrency()) {
    auto& castRelation = const_cast<std::vector<CastRelation>&>(leftRelation);
    auto& titleRelation = const_cast<std::vector<TitleRelation>&>(rightRelation);
    setMaxBitsToCompare(leftRelation.size());
    ThreadPool threadPool(numThreads);
    std::unordered_multimap<int32_t, MapType> partitions;
    partitions.rehash(numPartitionsToExpect * 2);
    std::mutex m_partitions;
    std::atomic_size_t castPartitionCounter(0);
    std::atomic_size_t titlePartitionCounter(0);
    threadPool.enqueue(castPartition,
                       std::ref(threadPool),
                       castRelation.begin(),
                       castRelation.end(),
                       0,
                       std::ref(m_partitions),
                       std::ref(partitions),
                       std::ref(castPartitionCounter)
    );
    threadPool.enqueue(titlePartition,
                       std::ref(threadPool),
                       titleRelation.begin(),
                       titleRelation.end(),
                       0,
                       std::ref(m_partitions),
                       std::ref(partitions),
                       std::ref(titlePartitionCounter)
            );
    while(size_t current = castPartitionCounter.load() < numPartitionsToExpect) {
        castPartitionCounter.wait(current);
    }
    while(size_t current = titlePartitionCounter.load() < numPartitionsToExpect) {
        titlePartitionCounter.wait(current);
    }
    std::cout << "numThreads: " << numThreads << '\n';
    return results;
}



#endif //PPDS_3_PARTITIONING_PARTITIONING_H
