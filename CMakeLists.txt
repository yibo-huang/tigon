cmake_minimum_required(VERSION 3.2)
project(star)

set(CMAKE_C_COMPILER /usr/bin/clang-15)
set(CMAKE_CXX_COMPILER /usr/bin/clang++-15)

set(CMAKE_C_STANDARD 14)
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -Wall -fno-omit-frame-pointer -pedantic  -O3 -DNDEBUG -g3 -Wno-long-long -Wno-unused-variable -Wno-unused-but-set-variable -Wno-variadic-macros -Wno-unused-lambda-capture")
set(CMAKE_EXE_LINKER_FLAGS "-lnuma -O3 -no-pie -fuse-ld=lld-15")

# -O3 -DNDEBUG
find_library(jemalloc_lib jemalloc) # jemalloc 5.0

# additional target to perform clang-format run, requires clang-format

# get all project files
file(GLOB_RECURSE ALL_SOURCE_FILES benchmark/*.h common/*.h core/*.h protocol/*.h bench*.cpp)

add_custom_target(
        format
        COMMAND clang-format
        -style=LLVM
        -i
        -sort-includes
        ${ALL_SOURCE_FILES}
)

include_directories(
        ${CMAKE_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/dependencies/cxl_shmem/src/cxlalloc/include
        ${CMAKE_SOURCE_DIR}/dependencies/cxl_shmem/src/common_cpp
)

file(GLOB_RECURSE MISC_CPP_FILES common/*.cpp protocol/Pasha/*.cpp protocol/SundialPasha/*.cpp core/*.cpp)
add_library(misc_cpp STATIC ${MISC_CPP_FILES})

if(APPLE)
    find_package(glog REQUIRED)
    find_package(gflags REQUIRED)
    target_link_libraries(misc_cpp ${jemalloc_lib} glog::glog gflags)
else()
    target_link_libraries(misc_cpp ${jemalloc_lib} glog gflags)
endif()

add_executable(bench_tpcc bench_tpcc.cpp)
target_link_libraries(bench_tpcc misc_cpp ${CMAKE_SOURCE_DIR}/dependencies/cxl_shmem/build_clang_release/libcxlalloc_static.a)

add_executable(bench_ycsb bench_ycsb.cpp)
target_link_libraries(bench_ycsb misc_cpp ${CMAKE_SOURCE_DIR}/dependencies/cxl_shmem/build_clang_release/libcxlalloc_static.a)