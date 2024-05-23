//
// Created by CatchACode on 17.05.24.
//

#ifndef PPDS_PARALLELISM_MERGESORT_H
#define PPDS_PARALLELISM_MERGESORT_H

#include "JoinUtils.hpp"
#include "ThreadPool.h"

template <typename RandomIt, typename Compare>
void merge(RandomIt left, RandomIt mid, RandomIt right, Compare comp) {
    std::vector<typename std::iterator_traits<RandomIt>::value_type> temp(std::distance(left, right));
    RandomIt i = left, j = mid, k = temp.begin();

    while (i != mid && j != right) {
        if (comp(*i, *j)) {
            *k++ = *i++;
        } else {
            *k++ = *j++;
        }
    }

    std::copy(i, mid, k);
    std::copy(j, right, k);
    std::move(temp.begin(), temp.end(), left);
}

template <typename RandomIt, typename Compare>
void merge_sort(ThreadPool& pool, RandomIt left, RandomIt right, Compare comp, size_t max_threads) {
    auto length = std::distance(left, right);
    if (length > 1) {
        RandomIt mid = left + length / 2;

        if (max_threads > 1) {
            auto leftFuture = pool.enqueue(merge_sort<RandomIt, Compare>, std::ref(pool), left, mid, comp, max_threads / 2);
            auto rightFuture = pool.enqueue(merge_sort<RandomIt, Compare>, std::ref(pool), mid, right, comp, max_threads / 2);

            leftFuture.wait();
            rightFuture.wait();
        } else {
            merge_sort(pool, left, mid, comp, max_threads);
            merge_sort(pool, mid, right, comp, max_threads);
        }

        merge(left, mid, right, comp);
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
