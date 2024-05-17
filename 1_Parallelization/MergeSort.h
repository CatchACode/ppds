//
// Created by CatchACode on 17.05.24.
//

#ifndef PPDS_PARALLELISM_MERGESORT_H
#define PPDS_PARALLELISM_MERGESORT_H

#include "JoinUtils.hpp"


#include <vector>
#include <thread>
#include <omp.h>

template <typename Relation>
void mergeSortOMPRecursive(std::vector<Relation>& v, unsigned long left, unsigned long right, const std::function<bool (const Relation& a, const Relation& b)> comparator) {
    if (left < right) {
        if (right-left >= 32) {
            unsigned long mid = (left+right)/2;
            #pragma omp taskgroup
            {
                #pragma omp task shared(v) untied if(right-left >= (1<<14))
                mergeSortRecursiveTitleRelation(v, left, mid);
                #pragma omp task shared(v) untied if(right-left >= (1<<14))
                mergeSortRecursiveTitleRelation(v, mid+1, right);
                #pragma omp taskyield
            }
            inplace_merge(v.begin()+left, v.begin()+mid+1, v.begin()+right+1, comparator);
        }else{
            sort(v.begin()+left, v.begin()+right+1, comparator);
        }
    }
}

template <typename Relation>
void mergeSortOMP(std::vector<Relation>& v, const std::function<bool (const Relation&, const Relation&)> comparator, int numThreads = std::thread::hardware_concurrency()) {
    omp_set_num_threads(numThreads);
    #pragma omp parallel
    #pragma omp single
    mergeSortRecursive<Relation>(v, 0, v.size()-1, comparator);
}

#endif //PPDS_PARALLELISM_MERGESORT_H
