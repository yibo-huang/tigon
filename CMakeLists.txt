cmake_minimum_required(VERSION 3.2)
project(tigon)

set(CMAKE_C_COMPILER /usr/bin/clang-15)
set(CMAKE_CXX_COMPILER /usr/bin/clang++-15)

set(CMAKE_C_STANDARD 14)
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(CMAKE_CXX_FLAGS "-pthread -Wall -fno-omit-frame-pointer -pedantic -O3 -march=native -DNDEBUG -g3 \
                    -Wno-long-long -Wno-unused-variable -Wno-unused-but-set-variable \
                    -Wno-variadic-macros -Wno-unused-lambda-capture -Wno-c99-extensions \
                    -Wno-zero-length-array -Wno-deprecated-declarations"
)
set(CMAKE_EXE_LINKER_FLAGS "-O3 -march=native -no-pie -fuse-ld=lld-15")

# include path
include_directories(
        ${CMAKE_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/dependencies/cxlalloc
)

find_library(jemalloc_lib jemalloc)

# all misc CPP files
file(GLOB_RECURSE MISC_CPP_FILES common/*.cpp protocol/Pasha/*.cpp protocol/SundialPasha/*.cpp protocol/TwoPLPasha/*.cpp core/*.cpp)
add_library(misc_cpp STATIC ${MISC_CPP_FILES})

# TPCC benchmark
add_executable(bench_tpcc bench_tpcc.cpp)
target_link_libraries(bench_tpcc misc_cpp ${CMAKE_SOURCE_DIR}/dependencies/cxlalloc/libcxlalloc_static.a ${jemalloc_lib} glog gflags)

# YCSB benchmark
add_executable(bench_ycsb bench_ycsb.cpp)
target_link_libraries(bench_ycsb misc_cpp ${CMAKE_SOURCE_DIR}/dependencies/cxlalloc/libcxlalloc_static.a ${jemalloc_lib} glog gflags)

# # SmallBank benchmark
# add_executable(bench_smallbank bench_smallbank.cpp)
# target_link_libraries(bench_smallbank misc_cpp ${CMAKE_SOURCE_DIR}/dependencies/cxlalloc/libcxlalloc_static.a ${jemalloc_lib} glog gflags)

# # TATP benchmark
# add_executable(bench_tatp bench_tatp.cpp)
# target_link_libraries(bench_tatp misc_cpp ${CMAKE_SOURCE_DIR}/dependencies/cxlalloc/libcxlalloc_static.a ${jemalloc_lib} glog gflags)
