//
// Created by klaas on 16.06.24.
//

#ifndef PPDS_2_MEMORY_HIERARCHY_CUSTOMALLOCATOR_H
#define PPDS_2_MEMORY_HIERARCHY_CUSTOMALLOCATOR_H

#include <cstdlib>
#include <iostream>

template<typename T>
class MallocAllocator {
public:
    using value_type = T;
    MallocAllocator() = default;

    template<typename U>
    MallocAllocator(const MallocAllocator<U>&) noexcept {}

    T* allocate(std::size_t n) {
        if(n==0) {
            return nullptr;
        } if (n > static_cast<std::size_t>(-1) / sizeof(T)) {
            throw std::bad_alloc();
        }
        void* p = malloc(n * sizeof(T));
        if (!p) {
            throw std::bad_alloc();
        }
        return static_cast<T*>(p);
    }
    void deallocate(T* p, std::size_t) noexcept {
        free(p);
    }

    template<typename U, typename... Args>
    void construct(U* p, Args&&... args) {
        std::cout << "Constructing with Args" << std::endl;
        //::new ((void*)p) U();
    }
    template <typename U>
    void destroy(U* p) {
        p->~U();
    }
};

template <typename T, typename U>
bool operator==(const MallocAllocator<T>&, const MallocAllocator<U>&) { return true; }

template <typename T, typename U>
bool operator!=(const MallocAllocator<T>&, const MallocAllocator<U>&) { return false; }

#endif //PPDS_2_MEMORY_HIERARCHY_CUSTOMALLOCATOR_H
