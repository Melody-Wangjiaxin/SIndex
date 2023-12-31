cmake_minimum_required (VERSION 3.5)
project (SIndex)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(JEMALLOC_DIR "/usr/lib/x86_64-linux-gnu")
set(MKL_LINK_DIRECTORY "/opt/intel/oneapi/mkl/2022.2.0/lib/intel64")
set(LINK_DIRECTORY "/opt/intel/oneapi/compiler/2022.2.0/linux/compiler/lib/intel64_lin")
set(MKL_INCLUDE_DIRECTORY "/opt/intel/oneapi/mkl/2022.2.0/include")

# Set a default build type if none was specified
# https://blog.kitware.com/cmake-and-the-default-build-type/
set(default_build_type "Release")
if(EXISTS "${CMAKE_SOURCE_DIR}/.git")
  set(default_build_type "Debug")
endif()

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to '${default_build_type}' as none was specified.")
  set(CMAKE_BUILD_TYPE "${default_build_type}" CACHE
      STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
    "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()

link_directories(${JEMALLOC_DIR})
link_directories(${MKL_LINK_DIRECTORY})
link_directories(${LINK_DIRECTORY})
link_libraries(jemalloc)

include_directories(${MKL_INCLUDE_DIRECTORY})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# add_compile_options(-Wall -fmax-errors=5 -march=native -mtune=native)
add_compile_options(-Wall -fmax-errors=5 -faligned-new -march=native -mtune=native)

# microbench
add_executable(microbench
    ${CMAKE_CURRENT_SOURCE_DIR}/microbench.cpp
)
string(TOLOWER ${CMAKE_BUILD_TYPE} _type)
if (_type STREQUAL release)
    set(default_build_type "Release")
    message(STATUS "Setting build type to '${default_build_type}' ")
    target_compile_definitions(microbench PRIVATE NDEBUGGING)
else()
    message(STATUS "Setting build type to '${default_build_type}' ")
endif()
target_link_libraries(microbench
    PRIVATE
        # mkl_rt
        -lmkl_intel_ilp64 
        -lmkl_intel_thread 
        -lmkl_core 
        -liomp5
        -lpthread 
        -lm 
        -ldl
)
