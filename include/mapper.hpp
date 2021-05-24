#pragma once
#include <vector>
#include <queue>
#include <string>
#include <memory>
#include <fstream>
#include <functional>
#include <thread>
#include <atomic>
#include <condition_variable>
#include <exception>

namespace reduse {
    template<typename key_type, typename value_type>
    class Mapper {
    private:

        const std::string input_filename;
        const std::string map_output_filename;
        std::ofstream map_output_file;

        const int num_mappers;
        std::atomic_bool isProducerDone;
        std::vector<std::thread> mp_threads;
        std::condition_variable buff_full, buff_empty;
        std::mutex buff_mutex;
        std::mutex output_file_mutex;
        std::string buff;

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
        // Initialize variables
        isProducerDone = false;
        map_output_file.open(map_output_filename);
        if(!map_output_file)
            throw std::runtime_error("Cannot open mapper output file: " + map_output_filename);

        // Initialize the consumers
        for (auto i = 0; i < num_mappers; i++)
            mp_threads[i] = std::thread{MAP};
        
        // Start the producer
        producer();

        // Wait for all the consumers to finish
        for (auto i = 0; i < num_mappers; i++)
            mp_threads[i].join();
        
        // Close output file
        map_output_file.close();
    }

    // Reads data from the file and gives it over to a mapper to process and write
    template<typename key_type, typename value_type>
    void Mapper<key_type, value_type>::producer() {
        // Open the input file

        std::istream input_file;
        input_file.open(input_filename);
        if(!input_file)
            throw std::runtime_error("Cannot open mapper input file: " + input_filename);

        // Producer starts writing here
        std::string input_line;

        while(std::getline(input_file,input_line)) {
            // Main producer logic
            std::unique_lock producer_lock{buff_mutex};
            buff_empty.wait(producer_lock, [&]() { return true; });
            buff = std::move(input_line);
            producer_lock.unlock();
            buff_full.notify_one();
        }

        // Mark producer done
        isProducerDone = true;
        
        // Must tell all sleeping consumers that the producer is done
        buff_full.notify_all();

        // Close input file
        input_file.close();
    }

    // Map worker
    template<typename key_type, typename value_type>
    void Mapper<key_type, value_type>::consumer() {
        std::string input_line;

        while(!isProducerDone) {
            // Main consumer logic
            
            // Get a new line
            std::unique_lock consumer_lock{buff_mutex}; 
            buff_full.wait(consumer_lock, [&]() { return true; });
            if (isProducerDone)
                break;
            input_line = std::move(buff);   
            consumer_lock.unlock();
            buff_empty.notify_one();

            // Process new line
            std::pair<key_type, value_type> new_map_pair = MAP(input_line);

            // Write emitted value to file
            std::scoped_lock file_lock{file_mutex};
            output << new_map_pair.first << ", " << new_map_pair.second << std::endl;
            file_lock.unlock();
        }   
    }
}
