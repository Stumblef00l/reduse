cmake_minimum_required(VERSION 3.20)

# Set the project name
project(reduse VERSION 1.0)

# Set C++ standard to 17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Set versions
set(reduse_VERSION_MAJOR 1 CACHE STRING "major version" FORCE)
set(reduse_VERSION_MINOR 0 CACHE STRING "minor version" FORCE)
set(reduse_VERSION ${reduse_VERSION_MAJOR}.${reduse_VERSION_MINOR} CACHE STRING "version" FORCE)

# Enable testing
enable_testing()

# Add subdirectories
add_subdirectory(tests)

# Get all the source files
set(HEADERS include/reduse/mapper.hpp include/reduse/reducer.hpp include/reduse/reduse.hpp)

# Add the libary target
add_library(reduse STATIC ${HEADERS})

# Set include directory for library
target_include_directories(reduse PUBLIC ${PROJECT_SOURCE_DIR}/include)

# Set include directory for interface reduse
set_target_properties(reduse PROPERTIES
    LINKER_LANGUAGE CXX
)

# Fetch libraries
set(THREADS_PREFERRED_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Link libraries
target_link_libraries(reduse PUBLIC Threads::Threads)

# Set destination directories
set(include_dest "include/reduse-${reduse_VERSION}")
set(main_lib_dest "lib/reduse-${reduse_VERSION}")
set(lib_dest "${main_lib_dest}/${CMAKE_BUILD_TYPE}")

#Install the library
install(TARGETS reduse DESTINATION "${lib_dest}")
install(FILES ${HEADERS} DESTINATION "${include_dest}")
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/config_impl.hpp DESTINATION "${lib_dest}")
