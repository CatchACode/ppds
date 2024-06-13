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
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")
set(CMAKE_CXX_FLAGS_RELEASE "-g -O2 -march=native -mtune=native -fPIC")

set(PPDS_PROJECT_DIR "${CMAKE_SOURCE_DIR}/..")

include_directories(${PPDS_PROJECT_DIR}/Util/include)
if(NOT DEFINED DATA_DIRECTORY)
    set(DATA_DIRECTORY "${PPDS_PROJECT_DIR}/DataGenerators/data/")
endif()

if(NOT DEFINED SOURCE_DIRECTORY)
    set(SOURCE_DIRECTORY "${CMAKE_SOURCE_DIR}/")
endif()

# Get the current Git commit hash
find_package(Git QUIET)

if(GIT_FOUND)
    execute_process(
            COMMAND ${GIT_EXECUTABLE} rev-parse --short HEAD
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            OUTPUT_VARIABLE GIT_COMMIT_HASH
            OUTPUT_STRIP_TRAILING_WHITESPACE
    )
else()
    set(GIT_COMMIT_HASH "unknown")
endif()

# Print the Git commit hash during configuration
message(STATUS "Git commit hash: ${GIT_COMMIT_HASH}")

# Custom target to print Git commit hash on every build
add_custom_target(print_git_hash ALL
        COMMAND ${CMAKE_COMMAND} -E echo "Git commit hash: ${GIT_COMMIT_HASH}"
        VERBATIM
)


# Load gtest
include(FetchContent)
include(ExternalProject)

FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest
    GIT_TAG        v1.14.0
    )

FetchContent_GetProperties(googletest)
if(NOT googletest_POPULATED)
    FetchContent_Populate(googletest)
    set(BUILD_SHARED_LIBS TRUE)  # Forces shared libraries, which implies -fPIC
    add_subdirectory(${googletest_SOURCE_DIR} ${googletest_BINARY_DIR})
endif()
FetchContent_MakeAvailable(googletest)



# Define the shared library
add_library(${PROJECT_ROOT} SHARED Join.cpp SortMergeJoin.h HashJoin.h NestedLoopJoin.h
        ThreadedLoad.h ../Util/include/generated_variables.h)

# Define the executable target that uses the shared library
add_executable(${PROJECT_EXECUTABLE} Join.cpp SortMergeJoin.h HashJoin.h NestedLoopJoin.h
        ThreadedLoad.h ../Util/include/generated_variables.h)

# Ensure the print_git_hash target runs before building the executable
add_dependencies(${PROJECT_EXECUTABLE} print_git_hash)

# Ensure the print_git_hash target runs before building the executable
add_dependencies(${PROJECT_ROOT} print_git_hash)

# Define a custom target that always runs
add_custom_target(run_python_script ALL)

# Ensure the run_python_script target runs before building the executable
add_dependencies(${PROJECT_EXECUTABLE} run_python_script)
add_dependencies(${PROJECT_ROOT} run_python_script)

# Define a custom command that runs the Python script
add_custom_command(
        TARGET run_python_script
        COMMAND ${CMAKE_COMMAND} -E echo "Running cache_sizes_to_file.py..."
        COMMAND ${PYTHON_EXECUTABLE} ${PPDS_PROJECT_DIR}/Util/cache_sizes_to_file.py ${PPDS_PROJECT_DIR}/Util/include/generated_variables.h
        COMMENT "Running Python script"
)

# Link with Libraries
find_package(OpenMP REQUIRED)
if (OpenMP_CXX_FOUND)
    message(STATUS "OpenMP found")
    target_link_libraries(${PROJECT_ROOT} PUBLIC OpenMP::OpenMP_CXX gtest_main)
    target_link_libraries(${PROJECT_EXECUTABLE} OpenMP::OpenMP_CXX gtest_main)
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
