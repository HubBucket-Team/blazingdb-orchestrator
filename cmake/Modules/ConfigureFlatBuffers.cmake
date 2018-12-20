#=============================================================================
# Copyright 2018 BlazingDB, Inc.
#     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
#=============================================================================

# BEGIN macros

macro(CONFIGURE_FLATBUFFERS_EXTERNAL_PROJECT)
    # Download and unpack flatbuffers at configure time
    configure_file(${CMAKE_SOURCE_DIR}/cmake/Templates/FlatBuffers.CMakeLists.txt.cmake ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/thirdparty/flatbuffers-download/CMakeLists.txt)

    execute_process(
        COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/thirdparty/flatbuffers-download/
    )

    if(result)
        message(FATAL_ERROR "CMake step for flatbuffers failed: ${result}")
    endif()

    execute_process(
        COMMAND ${CMAKE_COMMAND} --build . -- -j8
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/thirdparty/flatbuffers-download/
    )

    if(result)
        message(FATAL_ERROR "Build step for flatbuffers failed: ${result}")
    endif()
endmacro()

# END macros

# BEGIN MAIN #

if (FLATBUFFERS_INSTALL_DIR)
    message(STATUS "FLATBUFFERS_INSTALL_DIR defined, it will use vendor version from build ${FLATBUFFERS_INSTALL_DIR}")
else()
    message(STATUS "FLATBUFFERS_INSTALL_DIR not defined, it will be built from sources")
    configure_flatbuffers_external_project()
    set(FLATBUFFERS_INSTALL_DIR "${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/thirdparty/flatbuffers-install/")
endif()

message(STATUS "FLATBUFFERS_INSTALL_DIR: " ${FLATBUFFERS_INSTALL_DIR})

set(FLATBUFFERS_HOME ${FLATBUFFERS_INSTALL_DIR})
find_package(FlatBuffers REQUIRED)
set_package_properties(FlatBuffers
    PROPERTIES TYPE REQUIRED
    PURPOSE "FlatBuffers is an efficient cross platform serialization library."
    URL "https://google.github.io/flatbuffers/")

if (NOT FLATBUFFERS_FOUND)
    message(FATAL_ERROR "FlatBuffers not found, please check your settings.")
endif()

message(STATUS "flatbuffers installation found in ${FLATBUFFERS_INSTALL_DIR}")
message(STATUS "flatbuffers compiler found in ${FLATBUFFERS_INSTALL_DIR}/bin")

include_directories(${FLATBUFFERS_INCLUDEDIR} ${FLATBUFFERS_INCLUDE_DIR})
link_directories(${FLATBUFFERS_LIBDIR})

# END MAIN #
