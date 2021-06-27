cmake_minimum_required(VERSION 3.20)

# Set the project name
project(reduse)

# Set C++ standard to 17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Set versions
set(reduse_VERSION_MAJOR 1 CACHE STRING "major version" FORCE)
set(reduse_VERSION_MINOR 0 CACHE STRING "minor version" FORCE)
set(reduse_VERSION ${reduse_VERSION_MAJOR}.${reduse_VERSION_MINOR} CACHE STRING "version" FORCE)

# Config file
configure_file("config.hpp.in" "${CMAKE_CURRENT_BINARY_DIR}/config_impl.hpp")

# Set destination directories
include(GNUInstallDirs)

set(include_dest "${CMAKE_INSTALL_INCLUDEDIR}/reduse")
set(config_dest "${CMAKE_INSTALL_LIBDIR}/reduse/cmake")
set(lib_dest "${CMAKE_INSTALL_LIBDIR}")
set(additional_data_dest "${CMAKE_INSTALL_DATADIR}/reduse")

# Enable testing
enable_testing()

# Add subdirectories
add_subdirectory(tests)

# Get all the source files
set(HEADERS include/reduse/mapper.hpp include/reduse/reducer.hpp include/reduse/reduse.hpp include/reduse/config.hpp)

# Add the libary target
add_library(reduse STATIC ${HEADERS})

# Set include directory for library
target_include_directories(reduse PUBLIC 
    $<BUILD_INTERFACE:${reduse_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
    $<INSTALL_INTERFACE:${include_dest}>
    PRIVATE ${reduse_SOURCE_DIR}/include
)

# Fetch libraries
set(THREADS_PREFERRED_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Set linker language
set_target_properties(reduse PROPERTIES LINKER_LANGUAGE CXX)

# Link libraries
target_link_libraries(reduse PRIVATE Threads::Threads)

#Install the library
install(TARGETS reduse EXPORT reduse_targets 
    LIBRARY DESTINATION ${lib_dest}
    ARCHIVE DESTINATION ${lib_dest})

# Install the config file
install(EXPORT reduse_targets DESTINATION ${config_dest} FILE reduse-config.cmake)

# Install version ile
include(CMakePackageConfigHelpers)
set(version_file "${CMAKE_CURRENT_BINARY_DIR}/cmake/reduse-config-version.cmake")
write_basic_package_version_file(${version_file}
    VERSION ${reduse_VERSION}
    COMPATIBILITY AnyNewerVersion
)
install(FILES ${VERSION_FILE} DESTINATION ${config_dest})

# Install headers
install(FILES ${HEADERS} DESTINATION ${include_dest}/reduse)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/config_impl.hpp DESTINATION ${include_dest}/reduse)
