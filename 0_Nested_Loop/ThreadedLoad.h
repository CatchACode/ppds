//
// Created by CatchACode on 05.05.24.
//

#ifndef THREADEDLOAD_H
#define THREADEDLOAD_H

#include <vector>
#include <queue>
#include <iostream>
#include <cassert>
#include <thread>

#include "NestedLoopUtils.hpp"


template<typename Relation>
std::vector<Relation> threadedLoad(const std::string& filepath, const size_t& bufferSize = BLOCK_SIZE) {
    std::vector<Relation> data;
    std::queue<std::string> chunks;
    std::mutex m_data;
    std::mutex m_queue;
    std::mutex m_cout;
    std::condition_variable cv_queue;
    std::atomic<bool> stop(false);

    auto const workerThread = [&] {
        size_t numLine = 0;
        {
            std::lock_guard<std::mutex> lock(m_cout);
            std::cout << std::this_thread::get_id() << ": Started\n";
        }
        std::string chunk;
        std::istringstream raw;
        std::string line;
        Relation record{};
        while(!stop || !chunks.empty()) {
            std::unique_lock<std::mutex> l_queue(m_queue);
            cv_queue.wait(l_queue, [&] {return stop || chunks.empty(); });
            if(!chunks.empty()) { // As lock is created when stop is set, chunks might be empty when thread awakes
                chunk = chunks.front();
                chunks.pop();
                l_queue.unlock();
                assert(!chunk.empty());
                raw.str(chunk);
                while(!std::getline(raw, line).eof()) {
                    ++numLine;
                    if(parseLine(line, record)) {
                        std::lock_guard<std::mutex> l_data(m_data);
                        data.emplace_back(record);
                    } else {
                        std::lock_guard<std::mutex> l_cout(m_cout);
                        std::cerr << std::this_thread::get_id() << ": Error parsing line " << line << '\n';
                    }
                }
                raw.clear();
            }
        }
        m_cout.lock();
        std::cout << std::this_thread::get_id() << ": finished\n";
        m_cout.unlock();
    };
    std::vector<std::thread> threads(std::thread::hardware_concurrency());
    for (auto & i : threads) {
        i = std::thread(workerThread);
    }


    std::ifstream file(filepath, std::ios::in);
    if(!file) {
        m_cout.lock();
        std::cerr << std::this_thread::get_id() << ": Could not open " << filepath << '\n';
        m_cout.unlock();
        stop = true;
        cv_queue.notify_all();
        for(auto& t : threads) {
            t.join();
        }
        return data;
    }
    file.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    std::string readBuffer;
    readBuffer.resize(bufferSize);
    std::string leftovers;
    while(file) {
        file.read(readBuffer.data(), readBuffer.size());
        readBuffer.resize(file.gcount());
        if(!leftovers.empty()) {
            readBuffer.insert(readBuffer.begin(), leftovers.begin(), leftovers.end());
            leftovers.clear();
        }
        if(const auto newline = readBuffer.rfind('\n'); newline != std::string::npos) {
            leftovers.assign(readBuffer.begin() + newline + 1, readBuffer.end());
            readBuffer.resize(newline + 1);
        }
        {
            std::lock_guard<std::mutex> lock(m_queue);
            chunks.emplace(readBuffer);
            cv_queue.notify_one();
        }

    }
    stop.store(true);
    cv_queue.notify_all();
    for(auto& t: threads) {
        t.join();
    }
    return data;
}


inline std::vector<TitleRelation> threadedLoadTitleRelation(const std::string& filepath, const size_t& bufferSize) {
    return threadedLoad<TitleRelation>(filepath, bufferSize);
}

inline std::vector<CastRelation> threadedLoadCastRelation(const std::string& filepath, const size_t& bufferSize) {
    return threadedLoad<CastRelation>(filepath, bufferSize);
}

#endif //THREADEDLOAD_H
