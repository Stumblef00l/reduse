# ----- Tests -----

set(TEST_HEADERS "${reduse_SOURCE_DIR}/include")
set(TEST_LIBS gtest_main reduse)
set(TEST_SRC TestMapper.cpp TestReducer.cpp TestReduse.cpp)

configure_file("config.hpp.in" "${CMAKE_CURRENT_BINARY_DIR}/config_impl.hpp")

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
)

FetchContent_MakeAvailable(googletest)

add_executable(test_reduse ${TEST_SRC})
target_include_directories(test_reduse 
    PUBLIC 
    ${TEST_HEADERS}
    ${CMAKE_CURRENT_BINARY_DIR}
)

target_link_libraries(test_reduse PRIVATE ${TEST_LIBS})

include(GoogleTest)
gtest_discover_tests(test_reduse)