#pragma once
#include <functional>
#include <utility>

namespace reduse {
    template<typename map_key, typename map_value, typename reduce_value>
    void reduse(
        const int num_mappers,
        const int num_redusers,
        const std::string input_filename,
        const std::function<std::pair<map_key, map_value>(const std::string&)> &MAP, 
        const std::function<reduce_value(std::pair<map_key, map_value>)> &REDUCE,
        const bool verbose = false
    );
}
