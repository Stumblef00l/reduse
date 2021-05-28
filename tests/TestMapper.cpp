#include <iostream>
#include <fstream>
#include <utility>
#include <string>
#include <exception>
#include <unordered_map>
#include <gtest/gtest.h>
#include <config.h>
#include <reduse/mapper.hpp>

std::pair<int, int> MAP(const std::string& s) {
    int a = (s[0] - '0');
    int b = (s[1] - '0');
    return {a, b};
}

TEST(TestMapper, TestRun) {

    // Define the input file
    std::string input_filename = TEST_SOURCE_DIR;
    input_filename += "/testmap_input.txt";
    std::string output_filename = TEST_SOURCE_DIR;
    output_filename += "/testmap_output.txt";

    // Create the mapper and run it
    try {
        reduse::Mapper<int, int> mapper = {4, input_filename, output_filename, MAP};
        mapper.run();
    } catch (const std::exception &e) {
        std::cout << e.what();
        std::terminate();
    }
    // Read the file and test it
    std::fstream outfile_reader;
    outfile_reader.open(output_filename, std::ios::in);
    ASSERT_TRUE(outfile_reader.is_open());

    std::string output_line; // Single line of the file

    // We will use the these to test the file
    auto ct = 0;
    std::unordered_map<std::string, int> outfile_map;
    
    // Read and add each line to outfile_map
    while(std::getline(outfile_reader, output_line)) {
        ct++;
        outfile_map[output_line]++;
    }

    outfile_reader.close();
    ASSERT_FALSE(outfile_reader.is_open());
    
    ASSERT_EQ(ct, 2);
    ASSERT_EQ((int)(outfile_map.size()), 2);
    ASSERT_TRUE(outfile_map.find("1, 2") != outfile_map.end());
    ASSERT_TRUE(outfile_map.find("3, 4") != outfile_map.end());
}