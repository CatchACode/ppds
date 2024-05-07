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
void inline processLine(std::string& line, std::vector<Relation>& data, std::mutex& m_data,
                        Relation& record, std::mutex& m_cout) {
    if(parseLine(line, record)) {
        std::lock_guard l_data(m_data);
        data.emplace_back(record);
    } else {
        std::lock_guard l_cout(m_cout);
        std::cerr << std::this_thread::get_id() << "::threadedLoad::workerThread: Error parsing line " << line << std::endl;
    }
}

template<typename Relation>
void workerThread(std::mutex& m_cout, const std::atomic<bool>& stop, std::queue<std::string>& chunks, std::mutex& m_queue,
                  std::condition_variable& cv_queue, std::vector<Relation>& data, std::mutex& m_data) {
    {
        std::lock_guard lock(m_cout);
        std::cout << std::this_thread::get_id() << "::threadedLoad::workerThread: starting processing" << std::endl;
    }
    std::string chunk;
    std::istringstream raw;
    std::string line;
    Relation record{};
    while(!stop || !chunks.empty()) {
        std::unique_lock l_queue(m_queue);
        cv_queue.wait(l_queue, [&stop, &chunks] {return stop || chunks.empty(); });
        if(!chunks.empty()) { // As lock is created when stop is set, chunks might be empty when thread awakes
            chunk = chunks.front();
            chunks.pop();
            l_queue.unlock();
            assert(!chunk.empty());
            raw.str(chunk);
            while(!std::getline(raw, line).eof()) {
                processLine<Relation>(line, data, m_data, record, m_cout);
            }
            raw.clear();
        }
    }
    std::lock_guard l_cout(m_cout);
    std::cout << std::this_thread::get_id() << "::threadedLoad::workerThread: finished" << std::endl;
}


template<typename Relation>
std::vector<Relation> threadedLoad(const std::string& filepath, const size_t& bufferSize = BLOCK_SIZE) {
    std::vector<Relation> data;
    std::queue<std::string> chunks;
    std::mutex m_data;
    std::mutex m_queue;
    std::mutex m_cout;
    std::condition_variable cv_queue;
    std::atomic<bool> stop(false);

    std::vector<std::jthread> threads(4);
    for (auto & i : threads) {
        i = std::jthread(workerThread<Relation>, std::ref(m_cout), std::ref(stop), std::ref(chunks), std::ref(m_queue), std::ref(cv_queue), std::ref(data), std::ref(m_data));
    }


    std::ifstream file(filepath, std::ios::in);
    if(!file) {
        {
            std::scoped_lock l_cout(m_cout);
            std::cerr << std::this_thread::get_id() << "threadedLoad: Could not open " << filepath << std::endl;
        }
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
            std::scoped_lock lock(m_queue);
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
