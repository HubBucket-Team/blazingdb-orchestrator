# ZeroMQ cmake module
#
# The following import targets are created
#
# ::
#
#   libzmq-static
#   libzmq
#
# This module sets the following variables in your project::
#
#   ZeroMQ_FOUND - true if ZeroMQ found on the system
#   ZeroMQ_INCLUDE_DIR - the directory containing ZeroMQ headers
#   ZeroMQ_LIBRARY - 
#   ZeroMQ_STATIC_LIBRARY


####### Expanded from @PACKAGE_INIT@ by configure_package_config_file() #######
####### Any changes to this file will be overwritten by the next CMake run ####
####### The input file was ZeroMQConfig.cmake.in                            ########

macro(CONFIGURE_ZEROMQ_EXTERNAL_PROJECT)
    # Download and unpack zeromq at configure time
    configure_file(${CMAKE_SOURCE_DIR}/cmake/Templates/ZeroMQ.CMakeLists.txt.cmake ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/thirdparty/zeromq-download/CMakeLists.txt)

    execute_process(
        COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/thirdparty/zeromq-download/
    )

    if(result)
        message(FATAL_ERROR "CMake step for zeromq failed: ${result}")
    endif()

    execute_process(
        COMMAND ${CMAKE_COMMAND} --build . -- -j8
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/thirdparty/zeromq-download/
    )

    if(result)
        message(FATAL_ERROR "Build step for zeromq failed: ${result}")
    endif()

endmacro()

if (ZEROMQ_INSTALL_DIR)
    message(STATUS "ZEROMQ_INSTALL_DIR defined, it will use vendor version from ${ZEROMQ_INSTALL_DIR}")
    set(ZEROMQ_ROOT "${ZEROMQ_INSTALL_DIR}")
else()
    message(STATUS "ZEROMQ_INSTALL_DIR not defined, it will be built from sources")
    configure_ZEROMQ_external_project()
    set(ZEROMQ_ROOT "${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/thirdparty/zeromq-install/")
endif()


find_package(ZeroMQ REQUIRED)
set_package_properties(ZeroMQ PROPERTIES TYPE REQUIRED
    PURPOSE "ZeroMQ has the C++ protocol definitions for the BlazingSQL."
    URL "https://github.com/zeromq/libzmq.git")

if(NOT ZEROMQ_FOUND)
    message(FATAL_ERROR "zeromq not found, please check your settings.")
endif()

message(STATUS "ZEROMQ_ROOT found in ${ZEROMQ_ROOT}")
message(STATUS "ZEROMQ_INCLUDEDIR found in ${ZEROMQ_INCLUDEDIR}")
message(STATUS "ZEROMQ_LIBDIR found in ${ZEROMQ_LIBDIR}")
message(STATUS "ZEROMQ_STATIC_LIB found in ${ZEROMQ_STATIC_LIB}")

include_directories(${ZEROMQ_INCLUDEDIR})

link_directories(${ZEROMQ_LIBDIR})