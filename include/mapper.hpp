#pragma once
#include <vector>
#include <queue>
#include <string>
#include <memory>
#include <functional>
#include <thread>
#include <atomic>
#include <shared_mutex>
#include <exception>

namespace reduse {
    template<typename key_type, typename value_type>
    class Mapper {
    private:

        const int num_mappers;
        const std::string input_filename;
        const std::string map_output_filename;

        std::atomic_bool isProducerDone;
        std::vector<std::thread> mp_threads;
        std::queue<std::string> mp_queue;

        const std::function<std::pair<key_type, value_type>(const std::string&)> MAP;

        void consumer();
        void producer();

    public:

        using key_type = key_type;
        using value_type = value_type;

        Mapper(
            const int _num_mappers, 
            const std::string& _input_filename,
            const std::string& _map_output_filename,
            const std::function<std::pair<key_type, value_type>(const std::string&)>& _MAP
        );
        void run();
    };

    // Constructor
    template<typename key_type, typename value_type>
    Mapper<key_type, value_type>::Mapper(
        const int _num_mappers, 
        const std::string& _input_filename,
        const std::string& _map_output_filename,
        const std::function<std::pair<key_type, value_type>(const std::string&)>& _MAP
    ):  num_mappers(_num_mappers),
        input_filename(_filename),
        map_output_filename(_map_output_filename), 
        MAP(_MAP), 
        isProducerDone(false),
        mp_threads(std::vector<std::thread>(_num_mappers)) {}

    // Main running method
    template<typename key_type, typename value_type>
    void Mapper<key_type, value_type>::run() {
        isProducerDone = false;

        // Initialize the consumers
        for (auto i = 0; i < num_mappers; i++)
            mp_threads[i] = std::thread{MAP};
        
        // Start the producer
        producer();

        // Wait for all the consumers to finish
        for (auto i = 0; i < num_mappers; i++)
            mp_threads[i].join();
    }

    // Reads data from the file and gives it over to a mapper to process and write
    template<typename key_type, typename value_type>
    void Mapper<key_type, value_type>::producer() {}
}
