//
// Created by CatchACode on 17.06.24.
//

#ifndef PPDS_2_MEMORY_HIERARCHY_RINGBUFFER_H
#define PPDS_2_MEMORY_HIERARCHY_RINGBUFFER_H

#include <array>

template<typename T>
class RingBuffer {
public:
    /**
     * waits until an item is available to read. If multiple threads are waiting, only one is notified
     */
    void waitRead() {

    }

    /**
     * waits until a space is free to write. If multiple threads are waiting, only one is notified
     */
    void waitWrite() {

    }
    /**
     * @param T data returned
     * returns a copy of the oldest data in the ring buffer
     */
    T read() {

    }
private:
    static constexpr const std::size_t RINGBUFFER_SIZE = 256;
    std::array<T, RINGBUFFER_SIZE> buffer;
    std::size_t readPos;
    std::size_t writePos;
};

#endif //PPDS_2_MEMORY_HIERARCHY_RINGBUFFER_H
