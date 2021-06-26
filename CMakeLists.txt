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
)

# Fetch libraries
set(THREADS_PREFERRED_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Set include directory for library
target_include_directories(reduse PUBLIC 
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include> 
    $<INSTALL_INTERFACE:include>
)

# Set compile options for library
target_compile_options(reduse PUBLIC
    "-Wall" "-Werror" "-g"
)

# Link libraries
target_link_libraries(reduse PUBLIC Threads::Threads)

include(GNUInstallDirs)
install(
    TARGETS reduse
    EXPORT reduseTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

install(
    EXPORT reduseTargets
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/reduse
    NAMESPACE reduse::
    FILE reduseTargets.cmake
)

include(CMakePackageConfigHelpers)
configure_package_config_file(
    "Config.cmake.in"
    "reduseConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/reduse
    PATH_VARS
        CMAKE_INSTALL_LIBDIR
)

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/reduseConfigVersion.cmake
    VERSION 1.0.0
    COMPATIBILITY SameMajorVersion
)

### Install Config and ConfigVersion files
install(
    FILES "${CMAKE_CURRENT_BINARY_DIR}/reduseConfig.cmake"
          "${CMAKE_CURRENT_BINARY_DIR}/reduseConfigVersion.cmake"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/reduse"
)


# ----- Tests -----
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
)

FetchContent_MakeAvailable(googletest)

set(TEST_LIBS gtest_main reduse)
set(TEST_SRC tests/TestMapper.cpp tests/TestReducer.cpp tests/TestReduse.cpp)


enable_testing()

add_executable(test_reduse ${TEST_SRC})
target_include_directories(test_reduse 
    PUBLIC 
    ${PROJECT_BINARY_DIR}
)

target_compile_options(test_reduse PUBLIC
    "-Wall" "-Werror" "-g"
)

target_link_libraries(test_reduse PRIVATE ${TEST_LIBS})

include(GoogleTest)
gtest_discover_tests(test_reduse)