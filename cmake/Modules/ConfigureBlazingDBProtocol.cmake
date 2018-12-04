#=============================================================================
# Copyright 2018 BlazingDB, Inc.
#     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
#=============================================================================

# BEGIN macros

macro(CONFIGURE_BLAZINGDB_PROTOCOL_EXTERNAL_PROJECT)
    # Download and unpack blazingdb-protocol at configure time
    configure_file(${CMAKE_SOURCE_DIR}/cmake/Templates/BlazingDBProtocol.CMakeLists.txt.cmake ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/thirdparty/blazingdb-protocol-download/CMakeLists.txt)

    execute_process(
        COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/thirdparty/blazingdb-protocol-download/
    )

    if(result)
        message(FATAL_ERROR "CMake step for blazingdb-protocol failed: ${result}")
    endif()

    execute_process(
        COMMAND ${CMAKE_COMMAND} --build . -- -j8
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/thirdparty/blazingdb-protocol-download/
    )

    if(result)
        message(FATAL_ERROR "Build step for blazingdb-protocol failed: ${result}")
    endif()
endmacro()

# END macros

# BEGIN MAIN #

if (BLAZINGDB_PROTOCOL_INSTALL_DIR)
    message(STATUS "BLAZINGDB_PROTOCOL_INSTALL_DIR defined, it will use vendor version from ${BLAZINGDB_PROTOCOL_INSTALL_DIR}")
    set(BLAZINGDB_PROTOCOL_ROOT "${BLAZINGDB_PROTOCOL_INSTALL_DIR}")
else()
    message(STATUS "BLAZINGDB_PROTOCOL_INSTALL_DIR not defined, it will be built from sources")
    configure_blazingdb_protocol_external_project()
    set(BLAZINGDB_PROTOCOL_ROOT "${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/thirdparty/blazingdb-protocol-install/")
endif()

find_package(BlazingDBProtocol REQUIRED)
set_package_properties(BlazingDBProtocol PROPERTIES TYPE REQUIRED
    PURPOSE "BlazingDBProtocol has the C++ protocol definitions for the BlazingSQL."
    URL "https://github.com/BlazingDB/blazingdb-protocol")

if(NOT BLAZINGDB_PROTOCOL_FOUND)
    message(FATAL_ERROR "blazingdb-protocol not found, please check your settings.")
endif()

message(STATUS "blazingdb-protocol found in ${BLAZINGDB_PROTOCOL_ROOT}")
include_directories(${BLAZINGDB_PROTOCOL_INCLUDEDIR})

link_directories(${BLAZINGDB_PROTOCOL_LIBDIR})

# END MAIN #
