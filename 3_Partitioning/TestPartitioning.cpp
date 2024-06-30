//
// Created by klaas on 30.06.24.
//

#include <gtest/gtest.h>
#include "JoinUtils.hpp"
#include <bitset>

#include "Partitioning.h"
#include "../4_Strings/TimerUtil.hpp"

class TestPartitioning : public ::testing::Test {};

TEST_F(TestPartitioning, CastPartitioning) {
    const auto castRelation = load<CastRelation>(DATA_DIRECTORY + std::string("cast_info_zipfian1gb.csv"));

    auto result = radixCastPartition(castRelation, 8);

    for(const auto& record: result) {
        std::cout << std::bitset<32>(record.castInfoId) << std::endl;
    }
    std::cout << "Finished" << std::endl;
}

TEST_F(TestPartitioning, TitlePartitioning) {
    const auto titleRelation = loadTitleRelation(DATA_DIRECTORY + std::string("title_info_uniform1gb.csv"));
    Timer timer("Radix Title Partitioning");
    timer.start();
    auto result = radixTitlePartition(titleRelation, 8);
    timer.pause();
    std::cout << "Time: " << printString(timer)<< "ms" << std::endl;
    /*
    for(const auto& record: result) {
        std::cout << std::bitset<32>(record.titleId) << std::endl;
    }
     */
    std::cout << "Finished" << std::endl;
}