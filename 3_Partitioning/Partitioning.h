//
// Created by CatchACode on 24.06.24.
//

#ifndef PPDS_3_PARTITIONING_PARTITIONING_H
#define PPDS_3_PARTITIONING_PARTITIONING_H
#include "JoinUtils.hpp"
#include <omp.h>

static std::vector<size_t> castPartitionIndexes, titlePartitionIndexes;


std::vector<const CastRelation*> radixCastPartition(const std::vector<CastRelation>& castRelation, int numThreads) {
    omp_set_num_threads(numThreads);
    size_t n = castRelation.size();
    const size_t numBits = 3;
    const uint mask = (1 << numBits) - 1;
    const size_t numBuckets = 1 << numBits;

    std::vector<size_t> count(numBuckets, 0);
    std::vector<size_t> offset(numBuckets, 0);

    #pragma omp parallel
    {
        std::vector<size_t> localCount(numBuckets, 0);

        #pragma omp for
        for(size_t i = 0; i < n; i++) {
            localCount[castRelation[i].castInfoId & mask]++;
        }
        #pragma omp critical
        {
            for(size_t i = 0; i < numBuckets; i++) {
                count[i] += localCount[i];
            }
        }
    }
    castPartitionIndexes = std::vector<size_t>(numBuckets);
    castPartitionIndexes[0] = 0;
    for(int i = 1; i < numBuckets; ++i) {
       count[i] += count[i - 1];
       castPartitionIndexes[i] = count[i - 1];
    }
    std::vector<const CastRelation*> result(n);

    #pragma omp parallel for
    for(int i = castRelation.size() - 1; i >= 0; i--) {
        int position;
        #pragma omp atomic capture
        position = count[castRelation[i].castInfoId & mask]--;
        result[position] = &castRelation[i];
    }

    return std::move(result);
}

std::vector<const TitleRelation*> radixTitlePartition(const std::vector<TitleRelation>& titleRelation, int numThreads) {
    omp_set_num_threads(numThreads);
    size_t n = titleRelation.size();
    const size_t numBits = 3;
    const uint mask = (1 << numBits) - 1;
    const size_t numBuckets = 1 << numBits;

    std::vector<size_t> count(numBuckets, 0);
    std::vector<size_t> offset(numBuckets, 0);

    #pragma omp parallel
    {
        std::vector<size_t> localCount(numBuckets, 0);

        #pragma omp for
        for(size_t i = 0; i < n; i++) {
            localCount[titleRelation[i].titleId & mask]++;
        }
        #pragma omp critical
        {
            for(size_t i = 0; i < numBuckets; i++) {
                count[i] += localCount[i];
            }
        }
    }
    titlePartitionIndexes = std::vector<size_t>(numBuckets);
    titlePartitionIndexes[0] = 0;
    for(int i = 1; i < numBuckets; ++i) {
        count[i] += count[i - 1];
        titlePartitionIndexes[i] = count[i-1];
    }

    std::vector<const TitleRelation*> result(n);
    #pragma omp parallel for
    for(int i = titleRelation.size() - 1; i >= 0; i--) {
        int position;
        #pragma omp atomic capture
        position = count[titleRelation[i].titleId & mask]--;
        result[position] = &titleRelation[i];
    }

    return std::move(result);
}

#endif //PPDS_3_PARTITIONING_PARTITIONING_H
