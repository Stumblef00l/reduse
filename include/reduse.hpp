#pragma once
#include <functional>
#include <utility>
#include <mapper.hpp>
#include <reduser.hpp>

namespace reduse {
    template<typename map_key, typename map_value, typename reduse_value>
    void runReduse(
        const int num_mappers,
        const int num_redusers,
        const std::string input_filename,
        const std::function<std::pair<map_key, map_value>(const std::string&)> &MAP, 
        const std::function<reduse_value(std::pair<map_key, map_value>)> &REDUSE,
        const std::function<const int(const std::string&)> &BALANCER
    );
}
