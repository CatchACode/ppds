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


static std::atomic_bool titlePartitioned(false);
static std::atomic_bool castPartitioned(false);
static std::size_t maxBitsToCompare;
static std::size_t numPartitionsToExpect;

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


inline std::vector<int32_t>::iterator radixPartition(std::vector<int32_t>::iterator begin, std::vector<int32_t>::iterator end, uint8_t position) {
    auto zeroBin = begin;
    auto oneBin = end;
    while(zeroBin != oneBin) {
        if(getBitAtPosition(*zeroBin, position)) {
            std::swap(*zeroBin, *(--oneBin));
        } else {
            zeroBin++;
        }
    }
    return zeroBin;
}

inline void uint32Partition(std::shared_ptr<ThreadPool>& threadPool, std::vector<int32_t>::iterator begin, std::vector<int32_t>::iterator end, uint8_t position, std::atomic_size_t& counter) {
    if(position >= maxBitsToCompare) {
        counter++;
        counter.notify_all();
        //std::cout << "Counter is now " << current << std::endl;
        return;
    }

    auto split = radixPartition(begin, end, position);
    auto p1 = threadPool->enqueue(uint32Partition, threadPool, begin, split, position + 1, std::ref(counter));
    auto p2 = threadPool->enqueue(uint32Partition, threadPool, split, end, position + 1, std::ref(counter));
}

inline void uint32Partition(std::vector<int32_t>& vector) {
    maxBitsToCompare = 3;
    auto threadPool = std::make_shared<ThreadPool>(8);
    std::atomic_size_t counter(0);
    uint32Partition(threadPool, vector.begin(), vector.end(), 0, counter);
    size_t expected = 8;
    while(counter < expected) {
        counter.wait(expected - 1, std::memory_order_acquire);
    }
    std::cout << "counter: " << counter.load() << std::endl;
}

constexpr const std::size_t MAX_HASHMAP_SIZE = L2_CACHE_SIZE / (sizeof(int32_t) + sizeof(CastRelation*));

inline void setMaxBitsToCompare(const std::size_t sizeRelationVector) {
    //auto minimumNumOfHashMaps = std::ceil(sizeRelationVector / MAX_HASHMAP_SIZE);
    //if(minimumNumOfHashMaps == 0) {minimumNumOfHashMaps = 1;}
    //maxBitsToCompare = static_cast<std::size_t>(std::ceil(std::log2(minimumNumOfHashMaps)));
    maxBitsToCompare = sizeRelationVector;
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
                           uint8_t position, std::mutex& m_titlePartitions, std::vector<std::span<TitleRelation>>& titlePartitions,
                          std::atomic_size_t& counter) {
    if(position >= maxBitsToCompare) { // Write to the partition vector
        if(begin != end) {
            std::unique_lock lk (m_titlePartitions);
            titlePartitions[begin->titleId & bitmask()] = std::span<TitleRelation>(begin, end);
        }
        counter.fetch_add(1, std::memory_order_seq_cst);
        counter.notify_all();
        return;
    }
    auto split = titleRadixPartition(begin, end, position);
    threadPool.enqueue(titlePartition, std::ref(threadPool), begin, split, position + 1, std::ref(m_titlePartitions), std::ref(titlePartitions), std::ref(counter));
    threadPool.enqueue(titlePartition, std::ref(threadPool), split, end, position + 1, std::ref(m_titlePartitions), std::ref(titlePartitions), std::ref(counter));
}

void inline castPartition(ThreadPool& threadPool, const CastIterator begin, const CastIterator end,
                          uint8_t position, std::mutex& m_castPartitions, std::vector<std::span<CastRelation>>& castPartitions,
                          std::atomic_size_t& counter) {
    if(position >= maxBitsToCompare) {
        if(begin != end) {
            std::unique_lock lk(m_castPartitions);
            castPartitions[begin->movieId & bitmask()] = std::span<CastRelation>(begin, end);
        }
        counter++;
        counter.notify_all();
        return;
    }
    auto split = castRadixPartition(begin, end, position);
    threadPool.enqueue(castPartition, std::ref(threadPool), begin, split, position + 1, std::ref(m_castPartitions), std::ref(castPartitions), std::ref(counter));
    threadPool.enqueue(castPartition, std::ref(threadPool), split, end, position + 1, std::ref(m_castPartitions), std::ref(castPartitions), std::ref(counter));
}



void inline partition(ThreadPool& threadPool, std::vector<CastRelation>& leftRelation, std::vector<TitleRelation>& rightRelation,
                      std::vector<std::span<CastRelation>>& castPartitions, std::vector<std::span<TitleRelation>>& titlePartitions,
                      unsigned int numThreads = std::jthread::hardware_concurrency()) {
    std::vector<std::atomic_bool> finishedPartitions(numPartitionsToExpect);
    setMaxBitsToCompare(2);
    castPartitions.resize(numPartitionsToExpect);
    titlePartitions.resize(numPartitionsToExpect);
    std::mutex m_castPartitions;
    std::mutex m_titlePartitions;
    std::atomic_size_t castPartitionCounter(0);
    std::atomic_size_t titlePartitionCounter(0);
    threadPool.enqueue(castPartition, std::ref(threadPool), leftRelation.begin(), leftRelation.end(), 0, std::ref(m_castPartitions), std::ref(castPartitions), std::ref(castPartitionCounter));
    threadPool.enqueue(titlePartition, std::ref(threadPool), rightRelation.begin(), rightRelation.end(), 0, std::ref(m_titlePartitions), std::ref(titlePartitions), std::ref(titlePartitionCounter));
    while(size_t current = titlePartitionCounter.load() < numPartitionsToExpect) {
        titlePartitionCounter.wait(current);
    }
    while(size_t current = castPartitionCounter.load() < numPartitionsToExpect) {
        castPartitionCounter.wait(current);
    }
}

void hashJoin(std::span<CastRelation> leftRelation, std::span<TitleRelation> rightRelation, std::vector<ResultRelation>& results,
              std::mutex& m_results, std::atomic_size_t& counter) {
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
            std::scoped_lock lk(m_results);
            results.emplace_back(createResultTuple(record, *map[record.movieId]));
        }
    }
    /*
    std::scoped_lock lk(m_results);
    for(const auto& record: localResults) {
        results.emplace_back(record);
    }
     */
}

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

std::vector<ResultRelation> performPartitionJoin(const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation, unsigned int numThreads = std::jthread::hardware_concurrency()) {
    std::vector<CastRelation> castRelation(leftRelation);
    std::vector<TitleRelation> titleRelation(rightRelation);
    std::vector<std::span<CastRelation>> castPartitions;
    std::vector<std::span<TitleRelation>> titlePartitions;
    ThreadPool threadPool(numThreads);
    partition(threadPool, castRelation, titleRelation, castPartitions, titlePartitions, numThreads);
    assert(castPartitions.size() == titlePartitions.size());
    std::cout << "Number of Paritions: " << castPartitions.size() << '\n';
    std::cout << "finished Partitioning\n";
    std::cout << "Average cast partition size: " << averagePartitionSize(castPartitions) << '\n';
    std::cout << "Average title partition size: " << averagePartitionSize(titlePartitions) << '\n';
    std::vector<ResultRelation> results;
    results.reserve(leftRelation.size());
    std::mutex m_results;
    std::atomic_size_t counter(0);
    for(int i = 0; i < castPartitions.size(); ++i) {
        if(castPartitions.empty() || titlePartitions[i].empty()) {
            continue;
        }
        threadPool.enqueue(hashJoin, castPartitions[i], titlePartitions[i], std::ref(results), std::ref(m_results), std::ref(counter));
    }
    std::cout << "numThreads: " << numThreads << '\n';
    return results;
}



#endif //PPDS_3_PARTITIONING_PARTITIONING_H
