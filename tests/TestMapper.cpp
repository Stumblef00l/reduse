#define _REDUSE_MAIN_

#include <iostream>
#include <fstream>
#include <utility>
#include <vector>
#include <string>
#include <exception>
#include <unordered_map>
#include <gtest/gtest.h>
#include <reduse/config.hpp>
#include <reduse/mapper.hpp>

// Map method for the test TestRun
std::pair<int, std::string> MAP(const std::string& s) {
    auto a = (s[0] - '0');
    auto b = s.substr(1, s.length() - 1);
    return {a, b};
}

TEST(TestMapper, TestRun) {

    for(auto test_reps = 1; test_reps <= 500; test_reps++) {
        // Define the input file
        std::string input_filename = TEST_SOURCE_DIR;
        input_filename += "/testmap_input.txt";
        std::string output_filename = TEST_SOURCE_DIR;
        output_filename += "/testmap_output.txt";

        // Create the mapper and run it
        try {
            reduse::Mapper<int, std::string> mapper = {input_filename, output_filename, MAP, 4};
            mapper.run();
        } catch (const std::exception &e) {
            std::cout << e.what();
            std::terminate();
        }

        // Read the file and test it
        std::fstream outfile_reader;
        outfile_reader.open(output_filename, std::ios::in);
        ASSERT_TRUE(outfile_reader.is_open());

        // We will use the these to test the file
        auto ct = 0;
        std::unordered_map<int, std::vector<std::string>> outfile_map;
        
        // Read and add each line to outfile_map
        int key;
        std::string value;
        while(outfile_reader >> key) {
            ct++;
            outfile_reader >> value;
            outfile_map[key].push_back(value);
        }

        // Close the mapper output file
        outfile_reader.close();

        // Assertions
        ASSERT_FALSE(outfile_reader.is_open());
        ASSERT_EQ(ct, 5);
        ASSERT_EQ((int)(outfile_map.size()), 3);
        ASSERT_EQ((int)(outfile_map[1].size()), 2);
        ASSERT_EQ((int)(outfile_map[2].size()), 1);
        ASSERT_EQ((int)(outfile_map[3].size()), 2);
    }
}
