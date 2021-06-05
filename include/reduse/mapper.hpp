#pragma once
#include <vector>
#include <queue>
#include <string>
#include <iostream>
#include <fstream>
#include <functional>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <exception>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace reduse {
    template<typename key, typename value>
    class Mapper {
    private:

        const int num_mappers;
        const std::string input_filename;
        const std::string map_output_filename;
        const std::function<std::pair<key, value>(const std::string&)> MAP;

        std::fstream map_intermediate_file;
        std::atomic_bool isProducerDone;
        std::vector<std::thread> mp_threads;
        std::condition_variable buff_full, buff_empty;
        std::mutex buff_mutex;
        std::mutex output_file_mutex;
        bool produced;
        std::string buff;


        void consumer();
        void producer();
        void sortIntermediateFile();
        void groupKeys();

    public:

        using key_type = key;
        using value_type = value;

        Mapper(
            const int _num_mappers, 
            const std::string& _input_filename,
            const std::string& _map_output_filename,
            const std::function<std::pair<key_type, value>(const std::string&)>& _MAP
        );
        void run();
    };

    // Constructor
    template<typename key, typename value>
    Mapper<key, value>::Mapper(
        const int _num_mappers, 
        const std::string& _input_filename,
        const std::string& _map_output_filename,
        const std::function<std::pair<key, value>(const std::string&)>& _MAP
    ):  num_mappers(_num_mappers),
        input_filename(_input_filename),
        map_output_filename(_map_output_filename), 
        MAP(_MAP), 
        isProducerDone(false),
        mp_threads(std::vector<std::thread>(_num_mappers)) {}

    // Main running method
    template<typename key, typename value>
    void Mapper<key, value>::run() {
        // Initialize variables
        isProducerDone = false;
        produced = false;
        map_intermediate_file.open("intermediate_" + map_output_filename, std::ios::out);
        if(!map_output_file.is_open())
            throw std::runtime_error("Cannot open mapper output file: " + map_output_filename);

        // Initialize the consumers
        for (auto i = 0; i < num_mappers; i++)
            mp_threads[i] = std::thread(&Mapper<key, value>::consumer, this);
        
        // Start the producer
        std::thread producer_thread(&Mapper<key, value>::producer, this);

        // Wait for threads to finish
        producer_thread.join();
        for(auto &consumer_thread: mp_threads)
            consumer_thread.join();
        
        // Close output file
        map_intermediate_file.close();

        // Group the values in the map_output_file by key
        groupKeys();
    }

    // Reads data from the file and gives it over to a mapper to process and write
    template<typename key, typename value>
    void Mapper<key, value>::producer() {
        // Open the input file

        std::fstream input_file;
        input_file.open(input_filename, std::ios::in);
        if(!input_file.is_open())
            throw std::runtime_error("Cannot open mapper input file: " + input_filename);

        // Producer starts writing here
        std::string input_line;

        while(std::getline(input_file,input_line)) {
            // Main producer logic
            std::unique_lock producer_lock{buff_mutex};
            buff_empty.wait(producer_lock, [&]() { return !produced; });
            buff = std::move(input_line);
            produced = true;
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
    template<typename key, typename value>
    void Mapper<key, value>::consumer() {
        std::string input_line;

        while(!isProducerDone) {
            // Main consumer logic
            
            // Get a new line
            std::unique_lock consumer_lock{buff_mutex}; 
            buff_full.wait(consumer_lock, [&]() { return produced || isProducerDone; });
            if (isProducerDone && !produced)
                break;
            input_line = std::move(buff);
            produced = false;  
            consumer_lock.unlock();
            buff_empty.notify_one();

            // Process new line
            auto new_map_pair = MAP(std::ref(input_line));

            // Write emitted value to file
            std::scoped_lock file_lock{output_file_mutex};
            map_intermediate_file << "<" new_map_pair.first << ">, <" << new_map_pair.second << ">" << std::endl;
        }   
    }

    template<typename key, typename value>
    void Mapper<key, value>::groupKeys() {
        sortIntermediateFile();

        map_intermediate_file.open("intermediate_" + map_output_filename, std::ios::in);
        std::fstream map_output_file;
        map_output_file.open(map_output_filename, std::ios::out);

        std::string keyPairLine;
        std::string prevKey = "";

        while(std::getline(map_intermediate_file, keyPairLine)) {
            auto pos = keyPairLine.find('>, <');
            if(pos == std::string::npos)
                throw std::runtime_error("No , delimiter found for keyPair: " + keyPairLine);
            std::string newKey = keyPairLine.substr(1, pos - 1);
            std::string newVal = keyPairLine.substr(pos + 4, keyPairLine.length() - pos - 5);
            if(newKey.compare(prevKey) == 0)
                map_output_file << ", " << newVal;
            else {
                if(prevKey.length() > 0)
                   map_output_file << "}" << std::endl;
                map_output_file << "<" << newKey << ">, " << "{" << newVal;  
            }
        }
        map_output_file << "}" << std::endl;
        map_intermediate_file.close();
        map_output_file.close();
    }

    template<typename key, typename value>
    void Mapper<key, value>::sortIntermediateFile() {
        std::string intermediate_file = "intermediate_" + map_output_filename;
        if(fork() == 0) {
            execlp("sort", intermediate_file.c_str(), (char*)NULL);
        } else {
            wait(NULL);
        }
    }
}
