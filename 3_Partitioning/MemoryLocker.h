//
// Created by klaas on 28.06.24.
//

#ifndef PPDS_3_PARTITIONING_MEMORYLOCKER_H
#define PPDS_3_PARTITIONING_MEMORYLOCKER_H

#include <sys/mman.h>
#include <iostream>
#include <cerrno>
#include <cstring>
#include <sched.h>

void setRealtimePriority() {
    struct sched_param schedParam;
    schedParam.sched_priority = 1;

    if (sched_setscheduler(0, SCHED_FIFO, &schedParam) != 0) {
        std::cerr << "sched_setscheduler failed: " << std::strerror(errno) << std::endl;
    }
}



class MemoryLocker {
public:
    MemoryLocker(void* addr, size_t length) : addr_(addr), length_(length) {
        if (mlock(addr_, length_) != 0) {
            std::cerr << "mlock failed: " << std::strerror(errno) << std::endl;
        }
    }

    ~MemoryLocker() {
        if (munlock(addr_, length_) != 0) {
            std::cerr << "munlock failed: " << std::strerror(errno) << std::endl;
        }
    }

private:
    void* addr_;
    size_t length_;
};

#endif //PPDS_3_PARTITIONING_MEMORYLOCKER_H
