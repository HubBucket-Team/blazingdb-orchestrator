cmake_minimum_required(VERSION 3.3)

set(CMAKE_CXX_STANDARD 11)

# Include CMake modules
include(FeatureSummary)
include(CheckIncludeFiles)
include(CheckLibraryExists)
include(CTest)
include(ExternalProject)
find_package(Threads)

ExternalProject_Add(blazingdb-protocol_ep
	CMAKE_ARGS
		-DCMAKE_BUILD_TYPE=RELEASE
		-DCMAKE_INSTALL_PREFIX=blazingdb-protocol_prefix
	GIT_REPOSITORY git@github.com:BlazingDB/blazingdb-protocol.git
    GIT_TAG develop
    SOURCE_SUBDIR cpp
	UPDATE_COMMAND "")
ExternalProject_Get_property(blazingdb-protocol_ep BINARY_DIR)
set(BLAZINGDB_PROTOCOL_ROOT ${BINARY_DIR}/blazingdb-protocol_prefix)

file(MAKE_DIRECTORY ${BLAZINGDB_PROTOCOL_ROOT}/include)
file(MAKE_DIRECTORY ${BLAZINGDB_PROTOCOL_ROOT}/lib)

set(CMAKE_CXX_FLAGS "-pthread")

add_library(BlazingDB::Protocol INTERFACE IMPORTED)
add_dependencies(BlazingDB::Protocol blazingdb-protocol_ep)
target_include_directories(BlazingDB::Protocol
    INTERFACE ${BLAZINGDB_PROTOCOL_ROOT}/include)
target_link_libraries(BlazingDB::Protocol
    INTERFACE ${BLAZINGDB_PROTOCOL_ROOT}/lib/libblazingdb-protocol.a)

set(GOOGLE_FLATBUFFERS_ROOT
  ${BLAZINGDB_PROTOCOL_ROOT}/../google-flatbuffers_ep-prefix/src/google-flatbuffers_ep-build/google-flatbuffers_prefix)

file(MAKE_DIRECTORY ${GOOGLE_FLATBUFFERS_ROOT}/include)
file(MAKE_DIRECTORY ${GOOGLE_FLATBUFFERS_ROOT}/lib)

add_library(Google::Flatbuffers INTERFACE IMPORTED)
target_include_directories(Google::Flatbuffers
    INTERFACE ${GOOGLE_FLATBUFFERS_ROOT}/include)
target_link_libraries(Google::Flatbuffers
    INTERFACE ${GOOGLE_FLATBUFFERS_ROOT}/lib/libflatbuffers.a)

add_executable(blazingdb_orchestator_service src/orchestrator-service/orchestator-service.cc)
target_link_libraries(blazingdb_orchestator_service pthread BlazingDB::Protocol Google::Flatbuffers)
