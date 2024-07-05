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
#include <cassert>
#include <functional>
#include "MemoryLocker.h"
using CastIterator = std::vector<CastRelation>::iterator;
using TitleIterator = std::vector<TitleRelation>::iterator;

static std::size_t maxBitsToCompare;
static std::size_t numPartitionsToExpect;
static std::hash<int32_t> hasher;

static std::condition_variable cv_threads;
static std::mutex m_threads;
static std::atomic_size_t missingPartitions;

constexpr const std::size_t MAX_HASHMAP_SIZE = L2_CACHE_SIZE / (sizeof(int32_t) + sizeof(CastRelation*));



inline size_t averagePartitionSize(const std::vector<std::span<CastRelation>>& vector) {
    size_t sum = 0;
    for(const auto& span: vector) {
        sum += span.size();
    }
    return sum / vector.size();
}
inline size_t averagePartitionSize(const std::vector<std::span<TitleRelation>>& vector) {
    size_t sum = 0;
    for(const auto& span: vector) {
        sum += span.size();
    }
    return sum / vector.size();
}




/**
 * returns the bit in the number at pos
 */

inline bool getBitAtPosition(int32_t num, int pos) {
    // Create a mask by left-shifting 1 by pos positions
    int mask = 1 << pos;
    // Perform bitwise AND with the mask and shift right to get the bit at pos
    return (num & mask) >> pos;
}

inline void setMaxBitsToCompare(const std::size_t numThreads) {
    //auto minimumNumOfHashMaps = std::ceil(relationSize / MAX_HASHMAP_SIZE);
    //if(minimumNumOfHashMaps == 0) {minimumNumOfHashMaps = 1;}
    //maxBitsToCompare = static_cast<std::size_t>(std::ceil(std::log2(minimumNumOfHashMaps)));
    maxBitsToCompare = std::ceil(std::log(numThreads));
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

struct PartitionPair {
    std::atomic_bool alreadyStored = false;
    std::span<CastRelation> castSpan;
    std::span<TitleRelation> titleSpan;
};

inline void buildMap(const std::span<TitleRelation>& rightRelation, std::unordered_map<int32_t, const TitleRelation*>& map) {
    for(const auto& record: rightRelation) {
        map.emplace(record.titleId, &record);
    }
}

inline void probeMap(const std::span<CastRelation>& leftRelation, const std::unordered_map<int32_t, const TitleRelation*>& map,
                     std::vector<std::pair<const CastRelation*, const TitleRelation*>>& localResults) {
    const auto mapEnd = map.end();
    for(const auto& record: leftRelation) {
        auto it = map.find(record.movieId);
        if(it != mapEnd) {
            localResults.emplace_back(&record, it->second);
        }
    }
}

inline void
chunkProcessing(const std::span<CastRelation> &leftRelation, std::unordered_map<int32_t, const TitleRelation *> &map,
                std::vector<std::pair<const CastRelation *, const TitleRelation *>> &localResults) {
    auto chunkStart = leftRelation.begin();
    auto chunkEnd = leftRelation.begin();
    while(chunkStart != leftRelation.end()) {
        if(std::distance(chunkEnd, leftRelation.end()) < MAX_HASHMAP_SIZE) {
            chunkEnd = std::next(chunkEnd, MAX_HASHMAP_SIZE);
        } else {
            chunkEnd = leftRelation.end();
        }
        probeMap(leftRelation, map, localResults);
        map.clear();
        chunkStart = chunkEnd;
    }
}

inline void writeLocalResults(const std::vector<std::pair<const CastRelation*, const TitleRelation*>>& localResults,
                              std::vector<ResultRelation>& results, std::mutex& m_results) {
    std::lock_guard lk(m_results);
    for(const auto& [castPointer, titlePointer]: localResults) {
        results.emplace_back(createResultTuple(*castPointer, *titlePointer));
    }
}

inline void hashJoinMap(std::span<CastRelation> leftRelation, std::span<TitleRelation> rightRelation, std::vector<ResultRelation>& results,
                        std::mutex& m_results) {
    if (leftRelation.empty() || rightRelation.empty()) {
        return;
    }
    std::vector<std::pair<const CastRelation*, const TitleRelation*>> localResults;
    localResults.reserve(leftRelation.size());
    std::unordered_map<int32_t, const TitleRelation *> map;
    map.reserve(rightRelation.size());
    buildMap(rightRelation, map);
    chunkProcessing(leftRelation, map, localResults);
    writeLocalResults(localResults, results, m_results);
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
                           uint8_t position, std::vector<PartitionPair>& partitions, std::vector<ResultRelation>& results,
                           std::mutex& m_results) {
    if(position >= maxBitsToCompare) { // Write to the partition vector
        if(begin != end) {
            if(partitions[begin->titleId & bitmask()].alreadyStored.load(std::memory_order_acquire)) {
                // Something is already stored -> other Partition is filled
                hashJoinMap( partitions[begin->titleId & bitmask()].castSpan, std::span<TitleRelation>(begin, end), std::ref(results), std::ref(m_results));
                partitions[begin->titleId & bitmask()].alreadyStored.store(true, std::memory_order_release);
            } else {
                partitions[begin->titleId & bitmask()].titleSpan = std::span<TitleRelation>(begin, end);
                partitions[begin->titleId & bitmask()].alreadyStored.store(true, std::memory_order_release);
            }
        }
        return;
    }
    auto split = titleRadixPartition(begin, end, position);
    threadPool.enqueue(titlePartition, std::ref(threadPool), begin, split, position + 1, std::ref(partitions), std::ref(results), std::ref(m_results));
    titlePartition(std::ref(threadPool), split, end,position + 1, std::ref(partitions), std::ref(results), std::ref(m_results));
}

void inline castPartition(ThreadPool& threadPool, const CastIterator begin, const CastIterator end,
                          uint8_t position, std::vector<PartitionPair>& partitions, std::vector<ResultRelation>& results,
                          std::mutex& m_results) {
    if(position >= maxBitsToCompare) {
        if(begin != end) {
            if(partitions[begin->movieId & bitmask()].alreadyStored.load(std::memory_order_acquire)) {
                // Something is already stored! -> other Partition is filled
                hashJoinMap(std::span<CastRelation>(begin, end), partitions[hasher(begin->movieId) & bitmask()].titleSpan, std::ref(results), std::ref(m_results));
                partitions[begin->movieId & bitmask()].alreadyStored.store(true, std::memory_order_release);
            } else {
                partitions[begin->movieId & bitmask()].castSpan = std::span<CastRelation>(begin, end);
                partitions[begin->movieId & bitmask()].alreadyStored.store(true, std::memory_order_release);
            }
        }
        missingPartitions--;
        if(missingPartitions == 0) {
            cv_threads.notify_all();
        }
        return;
    }
    auto split = castRadixPartition(begin, end, position);
    threadPool.enqueue(castPartition, std::ref(threadPool), begin, split, position + 1, std::ref(partitions), std::ref(results), std::ref(m_results));
    castPartition(threadPool, split, end, position + 1, partitions, results, m_results);
}



void inline partition(ThreadPool& threadPool, std::vector<CastRelation>& leftRelation, std::vector<TitleRelation>& rightRelation,
                      std::vector<PartitionPair>& partitions, std::vector<ResultRelation>& results, std::mutex& m_results) {
    //castPartition(threadPool, leftRelation.begin(), leftRelation.end(), 0, partitions, results, m_results);
    threadPool.enqueue(castPartition, std::ref(threadPool), leftRelation.begin(), leftRelation.end(), 0, std::ref(partitions), std::ref(results), std::ref(m_results));
    titlePartition( std::ref(threadPool), rightRelation.begin(), rightRelation.end(), 0, std::ref(partitions), std::ref(results), std::ref(m_results));
}

void lockAllMemory() {
    if (mlockall(MCL_CURRENT | MCL_FUTURE) != 0) {
        std::cerr << "mlockall failed: " << std::strerror(errno) << std::endl;
    }
}

void unlockAllMemory() {
    if (munlockall() != 0) {
        std::cerr << "munlockall failed: " << std::strerror(errno) << std::endl;
    }
}

std::vector<ResultRelation> performPartitionJoin(const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation, unsigned int numThreads = std::jthread::hardware_concurrency()) {
    setMaxBitsToCompare(numThreads);
    missingPartitions.store(numPartitionsToExpect);
    auto& castRelation = const_cast<std::vector<CastRelation>&>(leftRelation);
    auto& titleRelation = const_cast<std::vector<TitleRelation>&>(rightRelation);
    //auto castRelation(leftRelation);
    //auto titleRelation(rightRelation);
    std::vector<PartitionPair> partitions(numPartitionsToExpect);
    ThreadPool threadPool(numThreads);
    std::vector<ResultRelation> results;
    results.reserve(26810);
    std::mutex m_results;
    partition(threadPool, castRelation, titleRelation, partitions, results, m_results);
    std::unique_lock l_threads(m_threads);
    cv_threads.wait(l_threads, []{return missingPartitions == 0;});
    return results;
}



#endif //PPDS_3_PARTITIONING_PARTITIONING_H
