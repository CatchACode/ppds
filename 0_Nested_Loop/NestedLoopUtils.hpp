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

#endif //NESTEDLOOPUTILS