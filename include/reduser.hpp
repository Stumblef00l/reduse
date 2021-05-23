#pragma once
#include <string>
#include <vector>
#include <thread>
#include <functional>

namespace reduse {
    template<typename map_key, typename map_value, typename reduse_value>
    class Reduser {
    private:

        const int num_redusers;

        std::vector<std::thread> rd_threads;
        std::queue<std::string> rd_queue;

        const std::function<reduse_value(map_key, map_value)> REDUSE;
        const std::function<const int(std::string)> BALANCER;

    public:

        using map_key_type = map_key;
        using map_value_type = map_value;
        using reduse_value_type = reduse_value;

        Reduser(
            const int _num_mappers, 
            const std::function<reduse_value(map_key, map_value)>& _REDUSE,
            const std::function<const int(const std::string&)>& _BALANCER
        );

        std::vector<reduse_value> run();
    };
}
