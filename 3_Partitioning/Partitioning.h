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
    std::shared_ptr<ThreadPool> threadPool = std::make_shared<ThreadPool>(8);
    std::atomic_size_t counter(0);
    uint32Partition(threadPool, vector.begin(), vector.end(), 0, counter);
    size_t expected = 8;
    while(counter < expected) {
        counter.wait(expected - 1, std::memory_order_acquire);
    }
    std::cout << "counter: " << counter.load() << std::endl;
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
inline std::vector<TitleRelation>::iterator titleRadixPartition(const TitleIterator& begin, const TitleIterator& end, const uint8_t& position) {
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

void inline titlePartition(const std::shared_ptr<ThreadPool>& threadPool, const TitleIterator& begin, const TitleIterator& end,
                           uint8_t position, std::mutex& m_titlePartitions, std::vector<std::span<TitleRelation>>& titlePartitions,
                          std::atomic_size_t& counter) {
    if(begin == end) { // write to partition hashmap
        return;
    }
    if(position >= maxBitsToCompare) { // Write to the partition vector
        return;
    }
    auto split = titleRadixPartition(begin, end, position);
}

void inline castPartition(const std::shared_ptr<ThreadPool>& threadPool, std::span<CastRelation> castRelation,
                          uint8_t position, std::mutex& m_castPartitions, std::vector<std::span<CastRelation>>& castPartitions,
                          std::atomic_size_t& counter) {

}

void inline partition(std::span<CastRelation> castRelation, std::span<TitleRelation> titleRelation, unsigned int numThreads = std::thread::hardware_concurrency()) {
    maxBitsToCompare = std::ceil(std::log2(numThreads));
    auto threadPool = std::make_shared<ThreadPool>(numThreads);

}





#endif //PPDS_3_PARTITIONING_PARTITIONING_H
