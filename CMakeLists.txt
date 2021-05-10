cmake_minimum_required(VERSION 3.20)

# Set the project name
project(reduse VERSION 0.3)

# Set the config file
configure_file(config.h.in config.h @ONLY)

# Get all the source files
set(SRC src/mapper.cpp src/reduser.cpp)
set(HEADERS include/mapper.hpp include/reduser.hpp include/reduse.hpp)

# Add the libary target
add_library(reduse STATIC ${SRC} ${HEADERS})

# Set include directory for library
target_include_directories(reduse PUBLIC ${PROJECT_SOURCE_DIR}/include ${PROJECT_BINARY_DIR})

# Set output directory for library
set_target_properties(reduse PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}
)
