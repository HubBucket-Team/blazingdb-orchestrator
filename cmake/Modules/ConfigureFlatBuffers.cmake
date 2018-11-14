#=============================================================================
# Copyright 2018 BlazingDB, Inc.
#     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
#     Copyright 2018 Cristhian Alberto Gonzales Castillo <cristhian@blazingdb.com>
#=============================================================================

include(ExternalProject)

ExternalProject_Add(google-flatbuffers_ep
  CMAKE_ARGS
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_INSTALL_PREFIX=google-flatbuffers_prefix
  GIT_REPOSITORY https://github.com/google/flatbuffers.git
  GIT_TAG v1.9.0
  UPDATE_COMMAND "")
ExternalProject_Get_property(google-flatbuffers_ep BINARY_DIR)
set(GOOGLE_FLATBUFFERS_ROOT ${BINARY_DIR}/google-flatbuffers_prefix)

file(MAKE_DIRECTORY ${GOOGLE_FLATBUFFERS_ROOT}/include)
file(MAKE_DIRECTORY ${GOOGLE_FLATBUFFERS_ROOT}/lib)

include_directories(${GOOGLE_FLATBUFFERS_ROOT}/include)

add_library(Google::Flatbuffers INTERFACE IMPORTED)
add_dependencies(Google::Flatbuffers google-flatbuffers_ep)
target_include_directories(Google::Flatbuffers
    INTERFACE ${GOOGLE_FLATBUFFERS_ROOT}/include)
target_link_libraries(Google::Flatbuffers
    INTERFACE ${GOOGLE_FLATBUFFERS_ROOT}/lib/libflatbuffers.a)
