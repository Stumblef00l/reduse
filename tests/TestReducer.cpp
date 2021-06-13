#include <iostream>
#include <fstream>
#include <utility>
#include <vector>
#include <string>
#include <exception>
#include <unordered_map>
#include <gtest/gtest.h>
#include <config.h>
#include <reduse/reducer.hpp>

// Reduce method for the Reducer
int REDUCE(int key, std::vector<int>& values) {
    auto sum = 0;
    for(auto &it: values)
        sum += it;
    return sum;
}

TEST(TestReducer, TestRun) {
    // Define the test filenames
    std::string map_output_filename = TEST_SOURCE_DIR;
    map_output_filename += "/testreducer_map_output.txt";
    std::string output_filename = TEST_SOURCE_DIR;
    output_filename += "/testreducer_output.txt";

    // Create a Reducer and run it
    try {
        reduse::Reducer<int, int, int> reducer = {map_output_filename, output_filename, REDUCE, 4};
        reducer.run();
    } catch (const std::exception &e) {
        std::cout << e.what();
        std::terminate();
    }
    
    // Open the output file produced by the reducer
    std::fstream output_file;
    output_file.open(output_filename, std::ios::in);

    auto ct = 0; // Number of items in the file
    std::unordered_map<int, int> output_file_map; // Frequency map for each item in the file
    int item; // Buffer for the current item in the file

    // Iterate over the output file and count number of items and each of their frequencies
    while(output_file >> item) {
        output_file_map[item]++;
        ct++;
    }

    // Assertions
    ASSERT_EQ(ct, 3);
    ASSERT_EQ((int)(output_file_map.size()), 2);
    ASSERT_EQ(output_file_map[323], 2);
    ASSERT_EQ(output_file_map[15], 1);
}
