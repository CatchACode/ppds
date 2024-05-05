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

#include <iostream>
#include <thread>
#include <format>
#include <chrono>
#include <gtest/gtest.h>

#include "NestedLoopUtils.hpp"



//==--------------------------------------------------------------------==//
//==------------------------- BENCHMARK TESTS --------------------------==//
//==--------------------------------------------------------------------==//

TEST(NestedLoopTest, TestParsingCastData) {
    std::cout << "Test reading cast data from a file.\n";

    const auto relation = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_uniform.csv"), 10);

    for(const auto& tuple : relation) {
        std::cout << castRelationToString(tuple) << '\n';
    }
}


TEST(NestedLoopTest, TestParsingTitleData) {
    std::cout << "Test reading data title from a file.\n";

    const auto relation = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform.csv"), 10);

    for(const auto& tuple : relation) {
        std::cout << titleRelationToString(tuple) << '\n';
    }
}


//==--------------------------------------------------------------------==//
//==------------------- TODO: JOIN IMPLEMENTATION ----------------------==//
//==--------------------------------------------------------------------==//


std::vector<ResultRelation> performSortedNestedLoopJoin(std::vector<CastRelation>& leftRelation, std::vector<TitleRelation>& rightRelation) {
    std::vector<ResultRelation> results;

    const auto sortCastRelation = [&] {
        std::ranges::sort(
        leftRelation.begin(),
        leftRelation.end(),
        [](const CastRelation& a, const CastRelation& b) { return a.movieId < b.movieId; }
        );
        std::cout << "Thread: " << std::this_thread::get_id() << " has finished sorting the CastRelation" << std::endl;
    };
    const auto sortTitleRelation = [&] {
        std::ranges::sort(
        rightRelation.begin(),
        rightRelation.end(),
        [](const TitleRelation& a, const TitleRelation& b) { return a.imdbId < b.imdbId; }
        );
        std::cout << "Thread: " << std::this_thread::get_id() << " has finished sorting the TitleRelation" << std::endl;
    };
    std::thread t1(sortCastRelation);
    std::thread t2(sortTitleRelation);

    t1.join();
    t2.join();

    auto l_it = leftRelation.begin();
    auto r_it = rightRelation.begin();
    while(l_it != leftRelation.end() && r_it != rightRelation.end()) {
        if(l_it->movieId < r_it->imdbId) {
            ++l_it;
        } else if (l_it->movieId > r_it->imdbId) {
            ++r_it;
        } else {
            results.emplace_back(createResultTuple(*l_it, *r_it));
            ++l_it;
            ++r_it;
        }
    }

    return results;
}

std::vector<ResultRelation> performNestedLoopJoin(const std::vector<CastRelation>& leftRelation, const std::vector<TitleRelation>& rightRelation) {
    std::vector<ResultRelation> results;
    for (const auto &left_entry: leftRelation) {
        for (const auto &right_entry: rightRelation) {
            if (left_entry.movieId == right_entry.imdbId) {
                results.emplace_back(createResultTuple(left_entry, right_entry));
            }
        }
    }

    return results;
}

TEST(NestedLoopTest, TestJoiningTuples) {
    std::cout << "Test reading data from a file.\n";

    const auto leftRelation = threadedLoadCastRelation(DATA_DIRECTORY + std::string("cast_info_uniform.csv"), BLOCK_SIZE);
    const auto rightRelation = threadedLoadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform.csv"), BLOCK_SIZE);

    const auto resultTuples = performNestedLoopJoin(leftRelation, rightRelation);

    std::cout << "\n\n#######################################\n";
    std::cout << "############### RESULTS ###############\n";
    std::cout << "#######################################\n";

    for(const auto& resultTuple : resultTuples) {
        std::cout << resultRelationToString(resultTuple) << '\n';
    }
    std::cout << "\n\n";
}

TEST(NestedLoopTest, TestJoiningTuplesSortedThreaded) {
    std::cout << "Test reading data from a file.\n";

    auto leftRelation = loadCastRelation(DATA_DIRECTORY + std::string("cast_info_uniform.csv"));
    auto rightRelation = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform.csv"));


    const auto resultTuples = performSortedNestedLoopJoin(leftRelation, rightRelation);

    std::cout << "\n\n#######################################\n";
    std::cout << "############### RESULTS ###############\n";
    std::cout << "#######################################\n";
    for(const auto& resultTuple : resultTuples) {
        std::cout << resultRelationToString(resultTuple) << '\n';
    }
    std::cout << "\n\n";
}

TEST(NestedLoopTest, TestJoinTuplesSortedThreadedLoadThreaded) {
    std::cout << "Test reading data from a file\n";

    auto leftRelation = threadedLoadCastRelation(DATA_DIRECTORY + std::string("cast_info_uniform.csv"), BLOCK_SIZE);
    auto rightRelation = threadedLoadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform.csv"), BLOCK_SIZE);

    const auto resultTuples = performSortedNestedLoopJoin(leftRelation, rightRelation);
    /*
    std::cout << "\n\n#######################################\n";
    std::cout << "############### RESULTS ###############\n";
    std::cout << "#######################################\n";
    for(const auto& resultTuple : resultTuples) {
        std::cout << resultRelationToString(resultTuple) << '\n';
    }
    */
    std::cout << "\n\n";

}

TEST(NestedLoopTest, TestLoadTimes) {
    std::cout << "Testing Load times\n";


    auto start = std::chrono::high_resolution_clock::now();
    auto leftRelation = threadedLoadCastRelation(DATA_DIRECTORY + std::string("cast_info_uniform.csv"), BLOCK_SIZE);
    auto rightRelation = threadedLoadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform.csv"), BLOCK_SIZE);
    auto stop = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);

    std::cout << "Time taken by threadedLoad: " << duration << "\n";

    start = std::chrono::high_resolution_clock::now();
    auto resultTuples = performSortedNestedLoopJoin(leftRelation, rightRelation);
    stop = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);

    std::cout << "Time taken to Merge: " << duration << "\n\n";
}

TEST(NestedLoopTest, TestLoadingCorrectness) {
    auto threadedLoad = threadedLoadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform.csv"), BLOCK_SIZE);
    auto sequentialLoad = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform.csv"));

    std::vector<TitleRelation> missing;
    //ASSERT_EQ(threadedLoad.size(), sequentialLoad.size()) << "Loaded different number of relations!\n";

    auto f = [&] (const TitleRelation& a, const TitleRelation& b) {
        return a.titleId < b.titleId;
    };

    auto f2 = [&] (const TitleRelation& a, const TitleRelation& b ) {
        return a.titleId != b.titleId;
    };

    std::ranges::sort(
        threadedLoad.begin(),
        threadedLoad.end(),
        f
        );
    std::ranges::sort(
        sequentialLoad.begin(),
        sequentialLoad.end(),
        f
        );

    std::ranges::set_difference(
        sequentialLoad.begin(),
        sequentialLoad.end(),
        threadedLoad.begin(),
        threadedLoad.end(),
        std::back_inserter(missing),
        f2
    );
    if(!missing.empty()) {
        std::cout << "Threaded load missed the following entries:\n";
        for(auto& a : missing) {
            std::cout << titleRelationToString(a) << "\n";
        }
    }


}

TEST(NestedLoopTest, TestBlockSize) {
    std::cout << "BlockSize is " << BLOCK_SIZE << std::endl;
}

TEST(NestedLoopTest, TestReadingLines) {
    std::vector<std::string> lines;
    std::ifstream file(DATA_DIRECTORY + std::string("cast_info_uniform.csv"));
    for(std::string line; std::getline(file, line);) {
        lines.emplace_back(line);
    }
    auto maxLength= std::ranges::max_element(
        lines.begin(),
        lines.end(),
        [](const auto& a, const auto& b) {
            return a.length() < b.length();
        }
    )->length();
    std::cout << maxLength;
}

TEST(NestedLoopTest, TestReadingFile) {
    auto readFile = [](size_t readSize) {
        std::ifstream file(DATA_DIRECTORY + std::string("title_info_uniform.csv"), std::ios::binary | std::ios::in);
        char buf[readSize + 1];
        memset(&buf, 0, readSize + 1);
        while(file) {
            file.read(buf, readSize);
            std::cout << buf;
        }
        file.close();
    };
    readFile(BLOCK_SIZE);
}