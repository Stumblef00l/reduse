cmake_minimum_required(VERSION 3.20)

# Set the project name
project(reduse VERSION 0.3)

# Set C++ standard to 17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Set the test source directory
set(TEST_SOURCE_DIR ${PROJECT_SOURCE_DIR}/tests)

# Set the config file
configure_file(config.h.in config.h @ONLY)

# Get all the source files
set(HEADERS include/reduse/mapper.hpp include/reduse/reducer.hpp include/reduse/reduse.hpp)

# Add the libary target
add_library(reduse STATIC ${HEADERS})

# Set include directory for interface reduse
set_target_properties(reduse PROPERTIES
    LINKER_LANGUAGE CXX
    INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_SOURCE_DIR}/include
)

# Fetch libraries
set(THREADS_PREFERRED_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Set include directory for library
target_include_directories(reduse PUBLIC ${PROJECT_SOURCE_DIR}/include/reduse ${PROJECT_BINARY_DIR})

# Set compile options for library
target_compile_options(reduse PUBLIC
    "-Wall" "-Werror" "-g"
)

# Link libraries
target_link_libraries(reduse PUBLIC Threads::Threads)


# ----- Tests -----
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
)

FetchContent_MakeAvailable(googletest)

set(TEST_LIBS gtest_main)
set(TEST_SRC tests/TestMapper.cpp tests/TestReducer.cpp)


enable_testing()

add_executable(test_reduse ${TEST_SRC})
target_include_directories(test_reduse 
    PUBLIC 
    ${PROJECT_BINARY_DIR}
)

target_compile_options(test_reduse PUBLIC
    "-Wall" "-Werror" "-g"
)

target_link_libraries(test_reduse PRIVATE ${TEST_LIBS} reduse)

include(GoogleTest)
gtest_discover_tests(test_reduse)