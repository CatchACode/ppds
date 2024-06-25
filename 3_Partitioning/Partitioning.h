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
using CastIterator = std::vector<CastRelation>::const_iterator;
using TitleIterator = std::vector<TitleRelation>::const_iterator;


static std::atomic_bool titlePartitioned(false);
static std::atomic_bool castPartitioned(false);

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


inline std::span<uint32_t>::iterator radixPartition(std::span<uint32_t> span, uint8_t position) {
    std::span<uint32_t>::iterator zeroBin = span.begin();
    std::span<uint32_t>::iterator oneBin = span.end();

    while(zeroBin != oneBin) {
        if(getBitAtPosition(*zeroBin, position)) {
            std::swap(*zeroBin, *(--oneBin));
        } else {
            zeroBin++;
        }
    }
    return zeroBin;
}

inline void uint32Partition(const std::shared_ptr<ThreadPool>& threadPool, std::span<uint32_t> span, uint8_t position, std::atomic_bool& stop) {
    if(position >= 4) {
        stop.store(true);
        stop.notify_all();
        return;
    }
    if(span.size() <= 1) {
        return;
    }
    auto split = radixPartition(span, position);
    std::span<uint32_t> left(span.begin(), std::distance(span.begin(),  split));
    std::span<uint32_t> right(split, std::distance(split, span.end()));
    auto p1 = threadPool->enqueue(uint32Partition, threadPool, left, position + 1, std::ref(stop));
    auto p2 = threadPool->enqueue(uint32Partition, threadPool, right, position + 1, std::ref(stop));

}

/**
 * @param castRelations a span containing the parition to sort
 * @param the bit position to sort by
 *
 * @return an iterator pointing to where the zero bin of the partition ends, ie start of ones bin
 */

inline std::vector<CastRelation>::const_iterator castRadixPartition(const CastIterator& begin, const CastIterator& end, const uint8_t& position) {
    auto zeroBin = begin;
    auto oneBin = end;
    while(zeroBin != oneBin && zeroBin != end) {
        if(getBitAtPosition(zeroBin->movieId, position)) { // if true 1 is at the bit position
            std::swap(zeroBin, (--oneBin));
        } else { // a zero is at bit position
            zeroBin++;
        }
    }
    return zeroBin;
}


inline std::span<TitleRelation>::iterator titleRadixPartition(std::span<TitleRelation> titleRelations, uint8_t position) {
    std::span<TitleRelation>::iterator zeroBin = titleRelations.begin();
    std::span<TitleRelation>::iterator oneBin = titleRelations.end();
    while(zeroBin != oneBin) {
        if(getBitAtPosition(zeroBin->titleId, position)) {
            std::swap(*zeroBin, *(--oneBin));
        } else {
            zeroBin++;
        }
    }
    return zeroBin;
}

void inline titlePartition(const std::shared_ptr<ThreadPool>& threadPool, std::span<TitleRelation> titleRelation, uint8_t position,
                           std::mutex& m_titlePartitions, std::vector<std::span<TitleRelation>>& titlePartitions,
                           std::atomic_size_t& counter) {
    if(titleRelation.size() <= 0) {
        return;
    }
    if(position >= 2) {
        std::unique_lock lk(m_titlePartitions);
        titlePartitions[titleRelation[0].titleId & 0b111] = titleRelation;
        titlePartitioned.store(true);
        titlePartitioned.notify_all();
        return;
    }
    if(titleRelation.size() <= 1) {
        return;
    }
    auto split = titleRadixPartition(titleRelation, position);
    long leftDistance = std::distance(titleRelation.begin(), split);
    long rightDistance = std::distance(split, titleRelation.end());
    std::span<TitleRelation> left(titleRelation.begin(), leftDistance);
    std::span<TitleRelation> right(split, rightDistance);
    auto p1 = threadPool->enqueue(titlePartition, std::ref(threadPool), left, position + 1, std::ref(m_titlePartitions),
                        std::ref(titlePartitions), std::ref(counter));
    auto p2 = threadPool->enqueue(titlePartition, std::ref(threadPool), right, position + 1, std::ref(m_titlePartitions),
                        std::ref(titlePartitions), std::ref(counter));
}

void inline castPartition(const std::shared_ptr<ThreadPool>& threadPool, std::span<CastRelation> castRelation,
                          uint8_t position, std::mutex& m_castPartitions, std::vector<std::span<CastRelation>>& castPartitions,
                          std::atomic_size_t& counter) {
    if(castRelation.size() <= 0) {
        return;
    }
    if(position >= 4) {
        // need to add this partition to partitions vector
        std::unique_lock lk(m_castPartitions);
        castPartitions[castRelation[0].movieId & 0b111] = castRelation;
        castPartitioned.store(true);
        castPartitioned.notify_all();
        return;
    }
    auto split = castRadixPartition(castRelation, position);
    std::span<CastRelation> left(castRelation.begin(), std::distance(castRelation.begin(), split));
    std::span<CastRelation> right(split, std::distance(split, castRelation.end()));
    auto p1 = threadPool->enqueue(castPartition, std::ref(threadPool), left, position + 1, std::ref(m_castPartitions),
                        std::ref(castPartitions), std::ref(counter));
    auto p2 = threadPool->enqueue(castPartition, std::ref(threadPool), right, position + 1, std::ref(m_castPartitions),
                        std::ref(castPartitions), std::ref(counter));
}

void inline partition(std::span<CastRelation> castRelation, std::span<TitleRelation> titleRelation, int numThreads = std::thread::hardware_concurrency()) {
    std::vector<std::span<CastRelation>> castPartitions(numThreads);
    castPartitions.reserve(16);
    std::vector<std::span<TitleRelation>> titlePartitions(numThreads);
    titlePartitions.reserve(16);
    std::mutex m_castPartitions;
    std::mutex m_titlePartitions;
    std::atomic_size_t counterCastPartitions(0);
    std::atomic_size_t counterTitlePartitions(0);
    auto threadPool = std::make_shared<ThreadPool>(7);
    threadPool->enqueue(castPartition, std::ref(threadPool), std::span(castRelation), 0, std::ref(m_castPartitions),
                        std::ref(castPartitions), std::ref(counterCastPartitions));
    /*
    threadPool->enqueue(titlePartition, std::ref(threadPool), std::span(titleRelation), 0, std::ref(m_titlePartitions),
                        std::ref(titlePartitions), std::ref(counterTitlePartitions));
                        */
    titlePartitioned.wait(false);
    castPartitioned.wait(false);
}

#endif //PPDS_3_PARTITIONING_PARTITIONING_H
