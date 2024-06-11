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
get_filename_component(PROJECT_ROOT ${CMAKE_SOURCE_DIR} NAME)
set(PROJECT_NAME "PPDS_${PROJECT_ROOT}")
set(PROJECT_EXECUTABLE "${PROJECT_ROOT}_EXECUTABLE")
message("Project name is: ${PROJECT_NAME}")

project(${PROJECT_NAME} VERSION 1.0.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set Optimization Flags
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -fPIC -march=native -mtune=native")
set(CMAKE_CXX_FLAGS_RELEASE "-g -O2 -march=native -mtune=native -fPIC")

set(PPDS_PROJECT_DIR "${CMAKE_SOURCE_DIR}/..")

include_directories(${PPDS_PROJECT_DIR}/Util/include)
if(NOT DEFINED DATA_DIRECTORY)
    set(DATA_DIRECTORY "${PPDS_PROJECT_DIR}/DataGenerators/data/")
endif()

if(NOT DEFINED SOURCE_DIRECTORY)
    set(SOURCE_DIRECTORY "${CMAKE_SOURCE_DIR}/")
endif()

# Load gtest
include(FetchContent)
include(ExternalProject)

set(BOOST_INCLUDE_LIBRARIES sort)
set(BOOST_ENABLE_CMAKE ON)

Set(FETCHCONTENT_QUIET FALSE)
FetchContent_Declare(
        Boost
        URL https://github.com/boostorg/boost/releases/download/boost-1.85.0/boost-1.85.0-cmake.7z # downloading a zip release speeds up the download
        USES_TERMINAL_DOWNLOAD TRUE
        GIT_PROGRESS TRUE
        DOWNLOAD_NO_EXTRACT FALSE
)
FetchContent_MakeAvailable(Boost)


FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest
    GIT_TAG        v1.14.0
    )

FetchContent_GetProperties(googletest)
if(NOT googletest_POPULATED)
    message(STATUS "googletest not populated!")
    FetchContent_Populate(googletest)
    set(BUILD_SHARED_LIBS TRUE)  # Forces shared libraries, which implies -fPIC
    add_subdirectory(${googletest_SOURCE_DIR} ${googletest_BINARY_DIR})
endif()
FetchContent_MakeAvailable(googletest)



# Define the shared library
add_library(${PROJECT_ROOT} SHARED Join.cpp SortMergeJoin.h HashJoin.h NestedLoopJoin.h)

# Define the executable target that uses the shared library
add_executable(${PROJECT_EXECUTABLE} Join.cpp SortMergeJoin.h HashJoin.h NestedLoopJoin.h)

# Link with Libraries
find_package(OpenMP REQUIRED)
if (OpenMP_CXX_FOUND)
    message(STATUS "OpenMP found")
    target_link_libraries(${PROJECT_ROOT} PUBLIC OpenMP::OpenMP_CXX gtest_main Boost::sort)
    target_link_libraries(${PROJECT_EXECUTABLE} OpenMP::OpenMP_CXX gtest_main Boost::sort)
else (OpenMP_CXX_FOUND)
    message(STATUS "OpenMP not found")
    message(STATUS "On Ubuntu, install the package by the following command!")
    message(STATUS "sudo apt install libomp-dev")
    target_link_libraries(${PROJECT_ROOT} PUBLIC gtest_main Boost::sort)
    target_link_libraries(${PROJECT_EXECUTABLE} gtest_main Boost::sort)
endif (OpenMP_CXX_FOUND)





# If necessary, include gtest include directories
target_include_directories(${PROJECT_ROOT} PRIVATE ${gtest_SOURCE_DIR}/include ${gtest_BINARY_DIR} Boost::sort)
target_include_directories(${PROJECT_EXECUTABLE} PRIVATE ${gtest_SOURCE_DIR}/include ${gtest_BINARY_DIR} Boost::sort)

# Add compile definitions
target_compile_definitions(${PROJECT_ROOT} PRIVATE
    DATA_DIRECTORY="${DATA_DIRECTORY}"
    SOURCE_DIRECTORY="${SOURCE_DIRECTORY}"
    )

target_compile_definitions(${PROJECT_EXECUTABLE} PRIVATE
    DATA_DIRECTORY="${DATA_DIRECTORY}"
    SOURCE_DIRECTORY="${SOURCE_DIRECTORY}"
    )
