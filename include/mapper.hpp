#pragma once
#include <vector>
#include <functional>
#include <string>
#include <queue>
#include <thread>

namespace reduse {
    template<typename K, typename V>
    class Mapper {
    private:

        const int num_mappers;
        const std::string input_filename;

        std::vector<std::thread> mp_threads;
        std::queue<std::string> mp_queue;

        const std::function<std::pair<K, V>(const std::string&)> MAP;
    public:

        using key_type = K;
        using value_type = V;

        Mapper(
            const int _num_mappers, 
            const std::string& _input_filename, 
            const std::function<std::pair<K, V>(const std::string&)>& _MAP
        );
        void run();
    };
}
