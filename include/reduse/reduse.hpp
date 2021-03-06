#pragma once
#include <iostream>
#include <vector>
#include <string>
#include <functional>
#include <utility>
#include <reduse/mapper.hpp>
#include <reduse/reducer.hpp>

namespace reduse {
    template<typename map_key, typename map_value, typename reduce_value>
    void reduse(
        const std::string input_filename,
        const std::string output_filename,
        const std::function<std::pair<map_key, map_value>(const std::string&)> &MAP, 
        const std::function<reduce_value(map_key, std::vector<map_value>&)> &REDUCE,
        const int num_mappers = DEFAULT_NUM_MAPPERS,
        const int num_reducers = DEFAULT_NUM_REDUCERS,
        const bool verbose = false
    ) {
        std::string map_output_filename = input_filename + "_map_output.txt";
        try {
            Mapper<map_key, map_value> mapper(input_filename, map_output_filename, MAP, num_mappers, verbose);
            mapper.run();
            Reducer<map_key, map_value, reduce_value> reducer(map_output_filename, output_filename, REDUCE, num_reducers, verbose);
            reducer.run();
            if(remove(map_output_filename.c_str()) != 0)
                throw std::runtime_error("Unable to delete " + map_output_filename);
        } catch (const std::exception &e) {
            std::cerr << e.what() << std::endl;
            remove(map_output_filename.c_str());
            std::terminate();
        }
    }
}
