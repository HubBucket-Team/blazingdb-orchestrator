#=============================================================================
# Copyright 2018 BlazingDB, Inc.
#     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
#     Copyright 2018 Alexander Ocsa <alexander@blazingdb.com>
#=============================================================================

cmake_minimum_required(VERSION 3.11)

project(blazingdb-protocol)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

message(STATUS "Using C++ standard: c++${CMAKE_CXX_STANDARD}")

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/" ${CMAKE_MODULE_PATH})
message(STATUS "CMAKE_MODULE_PATH:" "${CMAKE_MODULE_PATH}")

# Include CMake modules
include(FeatureSummary)
include(CheckIncludeFiles)
include(CheckLibraryExists)
include(CTest)

# NOTE Do not change the inclusion order
# Configure dependencies
include(ConfigureFlatBuffers)
include(ConfigureBlazingDBProtocol)
#include(ConfigureArrow)
include(ConfigureGoogleTest)

find_package(Threads)
set(CMAKE_CXX_FLAGS "-pthread")

add_executable(blazingdb_orchestator_service src/orchestrator-service/orchestator-service.cc)
target_link_libraries(blazingdb_orchestator_service pthread Google::Flatbuffers ${BLAZINGDB_PROTOCOL_STATIC_LIB})
