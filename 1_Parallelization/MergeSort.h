//
// Created by CatchACode on 17.05.24.
//

#ifndef PPDS_PARALLELISM_MERGESORT_H
#define PPDS_PARALLELISM_MERGESORT_H

#include "JoinUtils.hpp"


#include <vector>
#include <thread>
#include <omp.h>
#include <functional>




template <typename Relation>
void mergeSortOMPRecursive(std::vector<Relation>& v, unsigned long left, unsigned long right, const std::function<bool (const Relation& a, const Relation& b)>& comparator) {
    if (left < right) {
        if (right-left >= 32) {
            unsigned long mid = (left+right)/2;
            #pragma omp taskgroup
            {
                #pragma omp task shared(v) untied if(right-left >= (1<<14))
                mergeSortOMPRecursive<Relation>(v, left, mid, comparator);
                #pragma omp task shared(v) untied if(right-left >= (1<<14))
                mergeSortOMPRecursive<Relation>(v, mid+1, right, comparator);
                #pragma omp taskyield
            }
            inplace_merge(v.begin()+left, v.begin()+mid+1, v.begin()+right+1, comparator);
        }else{
            sort(v.begin()+left, v.begin()+right+1, comparator);
        }
    }
}
/**
 *
 * @tparam Relation type to Sort
 * @param v vector to sort
 * @param comparator function to compare vector elements
 * @param numThreads number of threads to use
 */

template <typename Relation>
void mergeSortOMP(std::vector<Relation>& v, const std::function<bool (const Relation&, const Relation&)>& comparator, int numThreads = std::jthread::hardware_concurrency()) {
    omp_set_num_threads(numThreads);
    #pragma omp parallel
    #pragma omp single
    mergeSortOMPRecursive<Relation>(v, 0, v.size()-1, comparator);
}


template<typename Relation>
void sortByChunks(std::vector<Relation>& relations, const std::function<bool (const Relation&, const Relation&)> comparator, int numThreads = std::jthread::hardware_concurrency()) {
    std::vector<std::pair<typename std::vector<Relation>::iterator, typename std::vector<Relation>::iterator>> partitionsIterators;

    std::vector<std::jthread> threads;

    size_t chunkSize = relations.size() / numThreads;
    typename std::vector<Relation>::iterator chunkStart = relations.begin();
    for(int i = 0; i < numThreads; ++i) {
        typename std::vector<Relation>::iterator chunkEnd;
        if(i == (numThreads -1)) {
            chunkEnd = relations.end();
        } else {
            chunkEnd = std::next(chunkStart, chunkSize);
        }

        partitionsIterators.emplace_back(std::pair(chunkStart, chunkEnd));
        threads.emplace_back(
                [&partitionsIterators, &comparator](int i){
                    std::sort(partitionsIterators[i].first, partitionsIterators[i].second, comparator);
                }, i);
        chunkStart = chunkEnd;
    } // We now have a both of sorted partitions
    for(auto& thread: threads) {
        thread.join();
    }

    typename std::vector<Relation>::iterator leftStart, leftEnd;
    leftStart = partitionsIterators[0].first;
    leftEnd = partitionsIterators[0].second;
    for(int i = 0; i < partitionsIterators.size(); ++i) {
        assert(std::is_sorted(partitionsIterators[i].first, partitionsIterators[i].second, comparator));
    }

    for(int i = 1; i < partitionsIterators.size(); ++i) {
        std::inplace_merge(leftStart, leftEnd, partitionsIterators[i].second, comparator);
        leftEnd = partitionsIterators[i].second;
    }


}

#endif //PPDS_PARALLELISM_MERGESORT_H
