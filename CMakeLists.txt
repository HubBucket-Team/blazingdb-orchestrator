#=============================================================================
# Copyright 2018 BlazingDB, Inc.
#     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
#     Copyright 2018 Alexander Ocsa <alexander@blazingdb.com>
#=============================================================================

cmake_minimum_required(VERSION 3.11)

project(blazingdb-orchestrator)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

message(STATUS "Using C++ standard: c++${CMAKE_CXX_STANDARD}")

if(CMAKE_COMPILER_IS_GNUCXX)
    option(CMAKE_CXX11_ABI "Enable the GLIBCXX11 ABI" ON)
    if(CMAKE_CXX11_ABI)
        message(STATUS "Enabling the GLIBCXX11 ABI")
    else()
        message(STATUS "Disabling the GLIBCXX11 ABI")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_GLIBCXX_USE_CXX11_ABI=0")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_GLIBCXX_USE_CXX11_ABI=0")
    endif(CMAKE_CXX11_ABI)
endif(CMAKE_COMPILER_IS_GNUCXX)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/" ${CMAKE_MODULE_PATH})
message(STATUS "CMAKE_MODULE_PATH:" "${CMAKE_MODULE_PATH}")

# Include CMake modules
include(FeatureSummary)
include(CheckIncludeFiles)
include(CheckLibraryExists)
include(CTest)

# NOTE Do not change the inclusion order
# Configure dependencies
include(ConfigureBoost)
include(ConfigureFlatBuffers)
include(ConfigureBlazingDBProtocol)
include(ConfigureBlazingDBCommunication)
include(ConfigureRapidJSON)

find_package(Threads)

# BEGIN MAIN #
include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${RAPIDJSON_INCLUDE_DIRS}
)

set(orchestrator_source_files
    src/config/BlazingConfig.cpp
    src/orchestrator-service/orchestator-service.cc)

add_executable(blazingdb_orchestator_service ${orchestrator_source_files})
target_link_libraries(blazingdb_orchestator_service Threads::Threads   ${ZEROMQ_STATIC_LIB} ${FLATBUFFERS_STATIC_LIB}  ${BLAZINGDB_PROTOCOL_STATIC_LIB} ${BLAZINGDB_COMMUNICATION_STATIC_LIB} Boost::system)
