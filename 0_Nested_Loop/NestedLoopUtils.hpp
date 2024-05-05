/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NESTEDLOOPUTILS_HPP
#define NESTEDLOOPUTILS_HPP

#include <cstring>
#include <fstream>
#include <iostream>
#include <format>
#include <filesystem>
#include <mutex>
#include <span>
#include <functional>
#include <string>
#include <iomanip>


//==--------------------------------------------------------------------==//
//==------------------ RELATION & RELATION UTILITY----------------------==//
//==--------------------------------------------------------------------==//

static constexpr size_t NUM_FIELD_CAST_RELATION = 7;
static constexpr size_t NUM_FIELDS_TITLE_RELATION = 12;

struct CastRelation {
    int32_t castInfoId;
    int32_t personId;
    int32_t movieId;
    int32_t personRoleId;
    char note[100];
    int32_t nrOrder;
    int32_t roleId;
};

// Production year would be a good to add (extract from title string).
struct TitleRelation {
    int32_t titleId;
    char title[200];
    char imdbIndex[12];
    int32_t kindId;
    int32_t productionYear;
    int32_t imdbId;
    char phoneticCode[5];
    int32_t episodeOfId;
    int32_t seasonNr;
    int32_t episodeNr;
    char seriesYears[49];
    char md5sum[32];
};

struct ResultRelation {
    int32_t titleId;
    char title[200];
    char imdbIndex[12];
    int32_t kindId;
    int32_t productionYear;
    int32_t imdbId;
    char phoneticCode[5];
    int32_t episodeOfId;
    int32_t seasonNr;
    int32_t episodeNr;
    char seriesYears[49];
    char md5sum[32];
    int32_t castInfoId;
    int32_t personId;
    int32_t movieId;
    int32_t personRoleId;
    char note[100];
    int32_t nrOrder;
    int32_t roleId;
};

[[nodiscard]] inline std::string titleRelationToString(const TitleRelation& relation) {
    std::ostringstream oss;
    oss<< relation.titleId << ","
       << relation.title << ","
       << relation.imdbIndex << ","
       << relation.kindId << ","
       << relation.productionYear << ","
       << relation.imdbId << ","
       << relation.phoneticCode << ","
       << relation.episodeOfId << ","
       << relation.seasonNr << ","
       << relation.episodeNr << ","
       << relation.seriesYears << ","
       << relation.md5sum;

    return oss.str();
}

[[nodiscard]] inline std::string castRelationToString(const CastRelation& relation) {
    std::ostringstream oss;
    oss << relation.castInfoId << ","
        << relation.personId << ","
        << relation.movieId << ","
        << relation.personRoleId << ","
        << relation.note << ","
        << relation.nrOrder << ","
        << relation.roleId;

    return oss.str();
}

[[nodiscard]] inline std::string resultRelationToString(const ResultRelation& relation) {
    std::ostringstream oss;
    oss << relation.titleId << ","
        << relation.title << ","
        << relation.imdbIndex << ","
        << relation.kindId << ","
        << relation.productionYear << ","
        << relation.imdbId << ","
        << relation.phoneticCode << ","
        << relation.episodeOfId << ","
        << relation.seasonNr << ","
        << relation.episodeNr << ","
        << relation.seriesYears << ","
        << relation.md5sum << ","
        << relation.castInfoId << ","
        << relation.personId << ","
        << relation.movieId << ","
        << relation.personRoleId << ","
        << relation.note << ","
        << relation.nrOrder << ","
        << relation.roleId;

    return oss.str();
}

inline void assignValueFromString(TitleRelation& titleRelation, const std::string& value, const size_t fieldIndex) {
    switch (fieldIndex) {
        case 0: titleRelation.titleId = std::stoi(value); break;
        case 1: strncpy(titleRelation.title, value.c_str(), 200); break;
        case 2: strncpy(titleRelation.imdbIndex, value.c_str(), 12); break;
        case 3: titleRelation.kindId = std::stoi(value); break;
        case 4: titleRelation.productionYear = std::stoi(value); break;
        case 5: titleRelation.imdbId = std::stoi(value); break;
        case 6: strncpy(titleRelation.phoneticCode, value.c_str(), 5); break;
        case 7: titleRelation.episodeOfId = std::stoi(value); break;
        case 8: titleRelation.seasonNr = std::stoi(value); break;
        case 9: titleRelation.episodeNr = std::stoi(value); break;
        case 10: strncpy(titleRelation.seriesYears, value.c_str(), 49); break;
        case 11: strncpy(titleRelation.md5sum, value.c_str(), 32); break;
        default: break;
    }
}

inline void assignValueFromString(CastRelation& castRelation, const std::string& value, const size_t fieldIndex) {
    switch (fieldIndex) {
        case 0: castRelation.castInfoId = std::stoi(value); break;
        case 1: castRelation.personId = std::stoi(value); break;
        case 2: castRelation.movieId = std::stoi(value); break;
        case 3: castRelation.personRoleId = std::stoi(value); break;
        case 4: strncpy(castRelation.note, value.c_str(), 100); break;
        case 5: castRelation.nrOrder = std::stoi(value); break;
        case 6: castRelation.roleId = std::stoi(value); break;
        default: break;
    }
}

//==--------------------------------------------------------------------==//
//==--------------------- DATASET LOADING LOGIC ------------------------==//
//==--------------------------------------------------------------------==//

inline bool parseLine(const std::string& line, TitleRelation& record) {
    std::istringstream ss(line);
    std::string field;
    size_t fieldIndex = 0;

    while (std::getline(ss, field, ',')) {
        if (fieldIndex >= NUM_FIELDS_TITLE_RELATION) {
            std::cerr << "Error: Too many fields in CSV line" << std::endl;
            return false;
        }
        assignValueFromString(record, field, fieldIndex);
        fieldIndex++;
    }

    if (fieldIndex != NUM_FIELDS_TITLE_RELATION) {
        std::cerr << "Error: Too few fields in CSV line" << std::endl;
        return false;
    }

    return true;
}

inline bool parseLine(const std::string& line, CastRelation& record) {
    std::istringstream ss(line);
    std::string field;
    size_t fieldIndex = 0;

    while (std::getline(ss, field, ',')) {
        if (fieldIndex >= NUM_FIELD_CAST_RELATION) {
            std::cerr << "Error: Too many fields in CSV line" << std::endl;
            return false;
        }
        assignValueFromString(record, field, fieldIndex);
        fieldIndex++;
    }

    if (fieldIndex != NUM_FIELD_CAST_RELATION) {
        std::cerr << "Error: Too few fields in CSV line" << std::endl;
        return false;
    }

    return true;
}

template <typename Relation>
std::vector<Relation> load(const std::string& filename, const size_t numberOfTuples = SIZE_MAX) {
    std::vector<Relation> data;
    data.reserve(8192);
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Error: Failed to open file " << filename << std::endl;
        exit(-1);
    }

    std::string line;
    bool firstLine = true;
    while (std::getline(file, line)) {
        if (firstLine) {
            firstLine = false;
            continue;
        }
        Relation record;
        if (parseLine(line, record)) {
            data.emplace_back(record);
        } else {
            std::cerr << "Error: Failed to parse line: " << line << std::endl;
        }

        if (data.size() >= numberOfTuples) {
            std::cout << "Loaded enough tuples. Returning now..." << std::endl;
            break;
        }
    }

    file.close();
    std::cout << "Loaded " << data.size() << " tuples from file." << std::endl;
    return data;
}

inline std::vector<TitleRelation> loadTitleRelation(const std::string& filename, const size_t numberOfTuples = SIZE_MAX) {
    return load<TitleRelation>(filename, numberOfTuples);
}

inline std::vector<CastRelation> loadCastRelation(const std::string& filename, const size_t numberOfTuples = SIZE_MAX) {
    return load<CastRelation>(filename, numberOfTuples);
}

inline ResultRelation createResultTuple(const CastRelation& cast, const TitleRelation& title) {
    ResultRelation result;
    // Assign values from title to result
    result.titleId = title.titleId;
    std::memcpy(result.title, title.title, 200);
    std::memcpy(result.imdbIndex, title.imdbIndex, 12);
    result.kindId = title.kindId;
    result.productionYear = title.productionYear;
    result.imdbId = title.imdbId;
    std::memcpy(result.phoneticCode, title.phoneticCode, 5);
    result.episodeOfId = title.episodeOfId;
    result.seasonNr = title.seasonNr;
    result.episodeNr = title.episodeNr;
    std::memcpy(result.seriesYears, title.seriesYears, 49);
    std::memcpy(result.md5sum, title.md5sum, 32);

    // Assign values from castInfo to result
    result.castInfoId = cast.castInfoId;
    result.personId = cast.personId;
    result.movieId = cast.movieId;
    result.personRoleId = cast.personRoleId;
    std::memcpy(result.note, cast.note, 100);
    result.nrOrder = cast.nrOrder;
    result.roleId = cast.roleId;

    return result;
}

std::streampos getFileSize(const std::string& filepath) {
    std::ifstream file(filepath, std::ifstream::ate | std::ifstream::binary);
    return file.tellg();
}

void findChunkBoundaries(const std::string& filepath, std::streampos& start, std::streampos& end) {
    std::cout << std::this_thread::get_id() <<"::findChunkBoundaries: " << filepath << "\n";
    std::ifstream file(filepath);
    file.seekg(start);
    std::string dummy;
    if(start == 0) {
        std::getline(file, dummy);  // When we start we need to skip the first csv line. Afters "start" is always the previous "end" so aligned to newline in file
    }
    std::cout << std::this_thread::get_id() << "::findChunkBoundaries: line starts with \'" << static_cast<char>(file.peek()) << "\'\n";
    start = file.tellg();
    file.seekg(end);
    std::getline(file, dummy);
    end = file.tellg();
    if(end == start) {
        // File is probably very small, so we initial start and stop where in the same line
        std::getline(file, dummy);
        end = file.tellg();
    }
    std::cout << std::this_thread::get_id() << "::findChunkBoundaries: line ends (after) \'" << static_cast<char>(file.peek())<< "\'" << std::endl;
    file.close();
}

template <typename Relation>
void processChunk(const std::string& chunk, std::istringstream& raw, std::vector<Relation>& data, std::mutex& m_data) {
    std::string line;
    Relation record;

}


template<typename Relation>
std::vector<Relation> threadedLoad(const std::string& filepath, const size_t& bufferSize, const size_t numberOfTuples = SIZE_MAX) {
    std::vector<Relation> data;
    std::queue<std::string> chunks;
    std::mutex m_data;
    std::mutex m_queue;
    std::mutex m_cout;
    std::condition_variable cv_queue;
    std::atomic<bool> stop(false);

    auto const workerThread = [&] {
        {
            std::lock_guard<std::mutex> lock(m_cout);
            std::cout << std::this_thread::get_id() << ": Started\n";
        }
        std::string chunk;
        std::istringstream raw;
        std::string line;
        Relation record;
        while(!stop || !chunks.empty()) {
            std::unique_lock<std::mutex> lock(m_queue);
            cv_queue.wait(lock, [&] {return stop || chunks.empty(); });
            if(!chunks.empty()) { // As lock is created when stop is set, chunks might be empty when thread awakes
                chunk = chunks.front();
                chunks.pop();
                lock.unlock();
                assert(!chunk.empty());
                raw.str(std::move(chunk));
                data.reserve( // Maybe this helps avoid unnecessary allocation, needs testing to see if benefical
                    std::ranges::count_if(chunk.begin(), chunk.end(), [&](const char c) {return c == '\n';}) + data.size()
                );
                while(!std::getline(raw, line).eof()) {
                    if(parseLine(line, record)) {
                        std::lock_guard<std::mutex> lock(m_data);
                        data.emplace_back(std::move(record));
                    } else {
                        std::lock_guard<std::mutex> lock(m_cout);
                        std::cerr << std::this_thread::get_id() << ": Error parsing line " << line << '\n';
                    }
                }
            }
        }
    };
    std::vector<std::thread> threads(std::thread::hardware_concurrency());
    for (size_t i = 0; i < threads.size(); ++i) {
        threads[i] = std::thread(workerThread);
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
    std::string readBuffer = "";
    readBuffer.resize(BLOCK_SIZE);
    std::string leftovers = "";
    while(file) {
        file.read(&readBuffer[0], readBuffer.size());
        if(!leftovers.empty()) {
            readBuffer.insert(
                readBuffer.begin(), leftovers.begin(), leftovers.end()
            );
            leftovers.clear();
        }
        const auto newline = readBuffer.rfind('\n');
        if(newline != std::string::npos) {
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


inline std::vector<TitleRelation> threadedLoadTitleRelation(const std::string& filepath, const size_t& bufferSize, const size_t numberOfTuples = SIZE_MAX) {
    return threadedLoad<TitleRelation>(filepath, bufferSize, numberOfTuples);
}

inline std::vector<CastRelation> threadedLoadCastRelation(const std::string& filepath, const size_t& bufferSize, const size_t numberOfFlags = SIZE_MAX) {
    return threadedLoad<CastRelation>(filepath, bufferSize, numberOfFlags);
}


#endif //NESTEDLOOPUTILS