#=============================================================================
# Copyright 2018 BlazingDB, Inc.
#     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
#     Copyright 2018 Alexander Ocsa <alexander@blazingdb.com>
#=============================================================================

cmake_minimum_required(VERSION 3.11)

project(blazingdb-orchestrator)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/" ${CMAKE_MODULE_PATH})
message(STATUS "CMAKE_MODULE_PATH: ${CMAKE_MODULE_PATH}")

# NOTE Import cmake modules from blazingdb-dependencies (Mandatory: First step!)
include(ImportCMakeModules)
include(ConfigureBlazingDBHostCompiler)

# Include CMake modules
include(FeatureSummary)
include(CheckIncludeFiles)
include(CheckLibraryExists)
include(CTest)

# NOTE Configure dependencies (always include this one first)
include(ConfigureBlazingDBDependencies)

# NOTE Do not change the inclusion order
# Configure dependencies
include(ConfigureBoost)
include(ConfigureFlatBuffers)
include(ConfigureBlazingDBProtocol)
include(ConfigureBlazingDBCommunication)
include(ConfigureRapidJSON)
include(ConfigureGDR)
include(ConfigureUCX)
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
target_link_libraries(blazingdb_orchestator_service Threads::Threads   ${ZEROMQ_STATIC_LIB} ${FLATBUFFERS_STATIC_LIB}  ${BLAZINGDB_PROTOCOL_STATIC_LIB} ${BLAZINGDB_COMMUNICATION_STATIC_LIB} Boost::system
    ${UCX_UCT_STATIC_LIB}
    ${GDR_STATIC_LIB}
    ${UCX_UCS_STATIC_LIB}
    ${UCX_UCM_STATIC_LIB}
    cuda /usr/local/cuda/lib64/libcudart.so
)

# Print the project summary
feature_summary(WHAT ALL INCLUDE_QUIET_PACKAGES FATAL_ON_MISSING_REQUIRED_PACKAGES)
