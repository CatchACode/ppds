//
// Created by CatchACode on 24.06.24.
//

#ifndef PPDS_3_PARTITIONING_PARTITIONING_H
#define PPDS_3_PARTITIONING_PARTITIONING_H
#include "JoinUtils.hpp"
#include <omp.h>


std::pair<std::vector<size_t>, std::vector<const CastRelation*>> radixCastPartition(const std::vector<CastRelation>& castRelation, int numThreads);

std::pair<std::vector<size_t>, std::vector<const TitleRelation*>> radixTitlePartition(const std::vector<TitleRelation>& titleRelation, int numThreads);

#endif //PPDS_3_PARTITIONING_PARTITIONING_H
