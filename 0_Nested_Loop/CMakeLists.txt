# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.21.0)
project(PPDS_NESTED_LOOP VERSION 1.0.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# Set Optimization Flags
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -fPIC -fno-omit-frame-pointer -std=c++20")
set(CMAKE_CXX_FLAGS_RELEASE "-g -O3 -march=native -mtune=native -fPIC -std=c++20")

set(PPDS_PROJECT_DIR "${CMAKE_SOURCE_DIR}/..")


include_directories(${PPDS_PROJECT_DIR}/Util/include)
if(NOT DEFINED DATA_DIRECTORY)
    set(DATA_DIRECTORY "${PPDS_PROJECT_DIR}/DataGenerators/data/")
endif()

if(NOT DEFINED SOURCE_DIRECTORY)
    set(SOURCE_DIRECTORY "${CMAKE_SOURCE_DIR}/")
endif()

# Determine block size
if(WIN32)
    execute_process(COMMAND "fsutil" "fsinfo" "drives" OUTPUT_VARIABLE DRIVE_INFO)
    string(REGEX MATCH "Bytes Per Sector.*: ([0-9]+)" BLOCK_SIZE_MATCH "${DRIVE_INFO}")
    set(BLOCK_SIZE ${CMAKE_MATCH_1})
elseif(APPLE)
    execute_process(COMMAND "diskutil" "info" "/" OUTPUT_VARIABLE DRIVE_INFO)
    string(REGEX MATCH "Device Block Size:.* ([0-9]+) Bytes" BLOCK_SIZE_MATCH "${DRIVE_INFO}")
    set(BLOCK_SIZE ${CMAKE_MATCH_1})
else()
    execute_process(COMMAND "stat" "-f" "-c" "%s" "/" OUTPUT_VARIABLE BLOCK_SIZE)
endif()


# Pass the block size to the source code as a preprocessor definition
add_compile_definitions(BLOCK_SIZE=${BLOCK_SIZE})
message("Block Size is ${BLOCK_SIZE}")

# Load gtest
include(FetchContent)
include(ExternalProject)

FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest
        GIT_TAG        v1.14.0
)

FetchContent_MakeAvailable(googletest)

# Define the shared library
add_library(0_Nested_Loop SHARED NestedLoop.cpp
        ../Util/include/Logger.h
        ThreadedLoad.h)

# Define the executable target that uses the shared library
add_executable(NestedLoopExecutable NestedLoop.cpp
        ../Util/include/Logger.h)

# Link executable and library with Google Test main
target_link_libraries(0_Nested_Loop gtest_main)
target_link_libraries(NestedLoopExecutable gtest_main)

# If necessary, include gtest include directories
target_include_directories(0_Nested_Loop PRIVATE ${gtest_SOURCE_DIR}/include ${gtest_BINARY_DIR})
target_include_directories(NestedLoopExecutable PRIVATE ${gtest_SOURCE_DIR}/include ${gtest_BINARY_DIR})

# Add compile definitions
target_compile_definitions(0_Nested_Loop PRIVATE
    DATA_DIRECTORY="${DATA_DIRECTORY}"
    SOURCE_DIRECTORY="${SOURCE_DIRECTORY}"
)

target_compile_definitions(NestedLoopExecutable PRIVATE
    DATA_DIRECTORY="${DATA_DIRECTORY}"
    SOURCE_DIRECTORY="${SOURCE_DIRECTORY}"
)
