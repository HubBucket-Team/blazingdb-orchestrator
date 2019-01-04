##=============================================================================
##
##  Copyright (c) Kitware, Inc.
##  All rights reserved.
##  See LICENSE.txt for details.
##
##  This software is distributed WITHOUT ANY WARRANTY; without even
##  the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
##  PURPOSE.  See the above copyright notice for more information.
##
##=============================================================================
# - Try to find ZeroMQ headers and libraries
#
# Usage of this module as follows:
#
#     find_package(ZeroMQ)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  ZEROMQ_ROOT  Set this variable to the root installation of
#                            ZeroMQ if the module has problems finding
#                            the proper installation path.
#
# Variables defined by this module:
#
#  ZeroMQ_FOUND              System has ZeroMQ libs/headers
#  ZeroMQ_LIBRARIES          The ZeroMQ libraries
#  ZeroMQ_INCLUDE_DIR        The location of ZeroMQ headers
#  ZeroMQ_VERSION            The version of ZeroMQ

find_path(ZEROMQ_ROOT
  NAMES include/zmq.h
  )

if(MSVC)
  #add in all the names it can have on windows
  if(CMAKE_GENERATOR_TOOLSET MATCHES "v140" OR MSVC14)
    set(_zmq_TOOLSET "-v140")
  elseif(CMAKE_GENERATOR_TOOLSET MATCHES "v120" OR MSVC12)
    set(_zmq_TOOLSET "-v120")
  elseif(CMAKE_GENERATOR_TOOLSET MATCHES "v110_xp")
    set(_zmq_TOOLSET "-v110_xp")
  elseif(CMAKE_GENERATOR_TOOLSET MATCHES "v110" OR MSVC11)
    set(_zmq_TOOLSET "-v110")
  elseif(CMAKE_GENERATOR_TOOLSET MATCHES "v100" OR MSVC10)
    set(_zmq_TOOLSET "-v100")
  elseif(CMAKE_GENERATOR_TOOLSET MATCHES "v90" OR MSVC90)
    set(_zmq_TOOLSET "-v90")
  endif()

  set(_zmq_versions
     "4_1_5" "4_1_4" "4_1_3" "4_1_2" "4_1_1" "4_1_0"
     "4_0_8" "4_0_7" "4_0_6" "4_0_5" "4_0_4" "4_0_3" "4_0_2" "4_0_1" "4_0_0"
     "3_2_5" "3_2_4" "3_2_3" "3_2_2"  "3_2_1" "3_2_0" "3_1_0")

  set(_zmq_release_names)
  set(_zmq_debug_names)
  foreach( ver ${_zmq_versions})
    list(APPEND _zmq_release_names "libzmq${_zmq_TOOLSET}-mt-${ver}")
  endforeach()
  foreach( ver ${_zmq_versions})
    list(APPEND _zmq_debug_names "libzmq${_zmq_TOOLSET}-mt-gd-${ver}")
  endforeach()

  #now try to find the release and debug version
  find_library(ZEROMQ_LIBPATH_RELEASE
    NAMES ${_zmq_release_names} zmq libzmq
    HINTS ${ZEROMQ_ROOT}/bin
          ${ZEROMQ_ROOT}/lib
    )

  find_library(ZEROMQ_LIBPATH_DEBUG
    NAMES ${_zmq_debug_names} zmq libzmq
    HINTS ${ZEROMQ_ROOT}/bin
          ${ZEROMQ_ROOT}/lib
    )

  if(ZEROMQ_LIBPATH_RELEASE AND ZEROMQ_LIBPATH_DEBUG)
    set(ZEROMQ_LIBPATH
        debug ${ZEROMQ_LIBPATH_DEBUG}
        optimized ${ZEROMQ_LIBPATH_RELEASE}
        )
  elseif(ZEROMQ_LIBPATH_RELEASE)
    set(ZEROMQ_LIBPATH ${ZEROMQ_LIBPATH_RELEASE})
  elseif(ZEROMQ_LIBPATH_DEBUG)
    set(ZEROMQ_LIBPATH ${ZEROMQ_LIBPATH_DEBUG})
  endif() 
else()
  find_library(ZEROMQ_LIBPATH
    NAMES zmq libzmq
    HINTS ${ZEROMQ_ROOT}/lib
    ) 
 
  set(ZEROMQ_SEARCH_LIB_PATH
  ${ZEROMQ_ROOT}/lib
  ${ZEROMQ_ROOT}/lib/x86_64-linux-gnu
  ${ZEROMQ_ROOT}/lib64
  ${ZEROMQ_ROOT}/build
  )


  find_library(ZEROMQ_STATIC_LIB NAMES libzmq.a
    PATHS ${ZEROMQ_SEARCH_LIB_PATH}
    NO_DEFAULT_PATH
    DOC "Path to ZEROMQ static library"
  )

  if (NOT ZEROMQ_STATIC_LIB)
    message(FATAL_ERROR "ZEROMQ includes and libraries NOT found. "
      "Looked for headers in ${ZEROMQ_ROOT}, "
      "and for libs in ${ZEROMQ_SEARCH_LIB_PATH}")
    set(ZEROMQ_FOUND FALSE)
  endif ()

endif()

find_path(ZeroMQ_INCLUDE_DIR
  NAMES zmq.h
  HINTS ${ZEROMQ_ROOT}/include
  )

find_path(ZEROMQ_LIBDIR
  NAMES libzmq.a
  HINTS ${ZEROMQ_ROOT}/lib
  )

function(extract_version_value value_name file_name value)
  file(STRINGS ${file_name} val REGEX "${value_name} .")
  string(FIND ${val} " " last REVERSE)
  string(SUBSTRING ${val} ${last} -1 val)
  string(STRIP ${val} val)
  set(${value} ${val} PARENT_SCOPE)
endfunction(extract_version_value)

extract_version_value("ZMQ_VERSION_MAJOR" ${ZeroMQ_INCLUDE_DIR}/zmq.h MAJOR)
extract_version_value("ZMQ_VERSION_MINOR" ${ZeroMQ_INCLUDE_DIR}/zmq.h MINOR)
extract_version_value("ZMQ_VERSION_PATCH" ${ZeroMQ_INCLUDE_DIR}/zmq.h PATCH)

set(ZeroMQ_VER "${MAJOR}.${MINOR}.${PATCH}")

#We are using the 2.8.10 signature of find_package_handle_standard_args,
#as that is the version that ParaView 5.1 && VTK 6/7 ship, and inject
#into the CMake module path. This allows our FindModule to work with
#projects that include VTK/ParaView before searching for Remus
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  ZeroMQ
  REQUIRED_VARS ZEROMQ_LIBPATH ZeroMQ_INCLUDE_DIR
  VERSION_VAR ZeroMQ_VER
  )

set(ZeroMQ_FOUND ${ZEROMQ_FOUND})
set(ZEROMQ_INCLUDEDIR ${ZeroMQ_INCLUDE_DIR})
set(ZeroMQ_LIBRARIES ${ZEROMQ_LIBPATH})
set(ZeroMQ_VERSION ${ZeroMQ_VER})

mark_as_advanced(
  ZEROMQ_ROOT
  ZEROMQ_LIBDIR
  ZEROMQ_LIBPATH
  ZEROMQ_LIBPATH_DEBUG
  ZEROMQ_LIBPATH_RELEASE
  ZeroMQ_INCLUDE_DIR
  ZEROMQ_STATIC_LIB
  ZeroMQ_VERSION
  )