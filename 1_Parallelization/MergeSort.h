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

/** performs worse than just sorting cast and title relations in a single thread with std::sort
 *
 * @tparam Relation
 * @param relations
 * @param comparator
 * @param numThreads
 */

template<typename Relation>
void inline cheapParallelSort(std::vector<Relation>& relations, std::function<bool (const Relation&, const Relation&)> comparator, const int numThreads = std::jthread::hardware_concurrency()) {
    switch (numThreads) {
        case 1: {
            std::ranges::sort(relations.begin(), relations.end(), comparator);
            return;
        }
        case 2: {
            const size_t size = relations.size();
            auto t1 = std::jthread([&relations, &size, &comparator] {
                std::ranges::sort(relations.begin(), relations.begin() + (size / 2), comparator);
            });
            auto t2 = std::jthread([&relations, &size, &comparator] {
                std::ranges::sort(relations.begin() + (size / 2), relations.end(), comparator);
            });
            t1.join();
            t2.join();
            std::ranges::inplace_merge(relations.begin(), relations.begin() + (size / 2), relations.end(), comparator);
            return;
        }
        case 4: {
            const size_t chunkSize = relations.size() / 4;
            auto t1 = std::jthread([&relations, &chunkSize, &comparator]{
                auto t11 = std::jthread([&relations, &chunkSize, &comparator] {
                        std::ranges::sort(relations.begin(), relations.begin() + chunkSize, comparator);
                });
                auto t12 = std::jthread([&relations, &chunkSize, &comparator] {
                    std::ranges::sort(relations.begin() + chunkSize, relations.begin() + 2 * chunkSize, comparator);
                });

                t11.join();
                t12.join();
                std::ranges::inplace_merge(relations.begin(), relations.begin() + chunkSize, relations.begin() + 2*chunkSize, comparator);
            });

            auto t2 = std::jthread([&relations, &chunkSize, &comparator] {
               auto t21 = std::jthread([&relations, &chunkSize, &comparator] {
                       std::ranges::sort(relations.begin() + 2*chunkSize, relations.begin() +3*chunkSize, comparator);
               });
               auto t22 = std::jthread([&relations, &chunkSize, &comparator] {
                   std::ranges::sort(relations.begin() + 3 * chunkSize, relations.end(), comparator);
               });

               t21.join();
               t22.join();
               std::ranges::inplace_merge(relations.begin() + 2*chunkSize, relations.begin() + 3*chunkSize, relations.end(), comparator);
            });
            std::ranges::inplace_merge(relations.begin(), relations.begin() + 2*chunkSize, relations.end(), comparator);
            return;
        }
        case 8: {
            const size_t chunkSize = relations.size() / 8;
            auto t1 = std::jthread([&relations, &comparator, &chunkSize] {
               auto t11 = std::jthread([&relations, &chunkSize, &comparator] {
                    auto t111 = std::jthread([&relations, &chunkSize, &comparator] {
                        std::ranges::sort(relations.begin(), relations.begin() + chunkSize, comparator);
                    });
                    auto t112 = std::jthread([&relations, &chunkSize, &comparator] {
                        std::ranges::sort(relations.begin() + chunkSize, relations.begin() + 2 * chunkSize, comparator);
                    });

                    t111.join();
                    t112.join();
                    std::ranges::inplace_merge(relations.begin(), relations.begin() + chunkSize, relations.begin() + 2*chunkSize, comparator);
               });
               auto t12 = std::jthread([&relations, &chunkSize, &comparator] {
                  auto t121 = std::jthread([&relations, &chunkSize, &comparator] {
                          std::ranges::sort(relations.begin() + 2*chunkSize, relations.begin() + 3*chunkSize, comparator);
                  });
                  auto t122 = std::jthread([&relations, &chunkSize, &comparator] {
                          std::ranges::sort(relations.begin() + 3*chunkSize, relations.begin() + 4*chunkSize, comparator);
                  });

                  t121.join();
                  t122.join();
                  std::ranges::inplace_merge(relations.begin() +2*chunkSize, relations.begin() +3*chunkSize, relations.begin() +4*chunkSize, comparator);
               });

               t11.join();
               t12.join();
               std::ranges::inplace_merge(relations.begin(), relations.begin() + 2*chunkSize, relations.begin() + 4*chunkSize, comparator);
            });
            auto t2 = std::jthread([&relations, &comparator, &chunkSize] {
                auto t21 = std::jthread([&relations, &chunkSize, &comparator] {
                    auto t211 = std::jthread([&relations, &chunkSize, &comparator] {
                            std::ranges::sort(relations.begin() +4*chunkSize, relations.begin() + 5*chunkSize, comparator);
                    });
                    auto t212 = std::jthread([&relations, &chunkSize, &comparator] {
                            std::ranges::sort(relations.begin() +5*chunkSize, relations.begin() + 6*chunkSize, comparator);
                    });

                    t211.join();
                    t212.join();
                    std::ranges::inplace_merge(relations.begin() + 4*chunkSize, relations.begin() + 5*chunkSize, relations.begin() + 6*chunkSize, comparator);
                });
                auto t22 = std::jthread([&relations, &chunkSize, &comparator] {
                    auto t221 = std::jthread([&relations, &chunkSize, &comparator] {
                            std::ranges::sort(relations.begin() + 6*chunkSize, relations.begin() + 7*chunkSize, comparator);
                    });
                    auto t222 = std::jthread([&relations, &chunkSize, &comparator] {
                            std::ranges::sort(relations.begin() + 7*chunkSize, relations.end(), comparator);
                    });

                    t221.join();
                    t222.join();
                    std::ranges::inplace_merge(relations.begin() +6*chunkSize, relations.begin() +7*chunkSize, relations.end(), comparator);
                });

                t21.join();
                t22.join();
                std::ranges::inplace_merge(relations.begin() +4*chunkSize, relations.begin() + 6*chunkSize, relations.end(), comparator);
            });
            t1.join();
            t2.join();

            std::ranges::inplace_merge(relations.begin(), relations.begin() + 4*chunkSize, relations.end(), comparator);
            return;
        }
        default:
            std::cout << "cheapParallelMerge: no valid numThread! simply sorting now\n";
            std::ranges::sort(relations.begin(), relations.end(), comparator);
            return;
    }

}


#endif //PPDS_PARALLELISM_MERGESORT_H
