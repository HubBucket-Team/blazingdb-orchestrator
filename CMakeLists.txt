cmake_minimum_required(VERSION 3.3)

set(CMAKE_CXX_STANDARD 11)

include(ExternalProject)

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

add_executable(calcite_client src/blazingdb/protocol/calcite-client.cc)
target_link_libraries(calcite_client BlazingDB::Protocol Google::Flatbuffers)

add_executable(interpreter_client src/blazingdb/protocol/interpreter-client.cc)
target_link_libraries(interpreter_client BlazingDB::Protocol Google::Flatbuffers)


add_executable(interpreter_server src/blazingdb/protocol/interpreter-server.cc)
target_link_libraries(interpreter_server BlazingDB::Protocol Google::Flatbuffers)
