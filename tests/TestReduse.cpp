#include <iostream>
#include <fstream>
#include <utility>
#include <vector>
#include <string>
#include <exception>
#include <unordered_map>
#include <gtest/gtest.h>
#include <reduse/config.hpp>
#include <reduse/reduse.hpp>

// Map method
std::pair<int, int> REDUSE_MAP(const std::string& s) {
    auto a = (s[0] - '0');
    auto b = s.substr(1, s.length() - 1);
    return {a, stoi(b)};
}

// Reduce method
int REDUSE_REDUCE(int key, std::vector<int>& values) {
    auto sum = 0;
    for(auto &it: values)
        sum += it;
    return sum;
}

TEST(TestReduse, TestRun) {

    for(auto test_reps = 1; test_reps <= 500; test_reps++) {
        // Define the input file
        std::string input_filename = TEST_SOURCE_DIR;
        input_filename += "/testreduse_input.txt";
        std::string output_filename = TEST_SOURCE_DIR;
        output_filename += "/testreduse_output.txt";

        // Create the mapper and run it
        try {
            reduse::reduse<int, int, int> (input_filename, output_filename, REDUSE_MAP, REDUSE_REDUCE, 3, 3);
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
}