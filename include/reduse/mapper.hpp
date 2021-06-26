#pragma once
#include <vector>
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

    const int DEFAULT_NUM_MAPPERS = 1; // Default number of mapper workers

    /** @brief Handles the entire map phase
     * @param key Data type of the key emitted by the MAP method
     * @param value Data type of the value emitted by the MAP method
     */
    template<typename key, typename value>
    class Mapper {
    private:

        const std::string input_filename; // Input filename
        const std::string map_output_filename; // Mapper's intermediate output filename
        const std::function<std::pair<key, value>(const std::string&)> MAP; // Mapper routine
        const int num_mappers; // Number of mappers
        const bool verbose; // Verbose output to console

        std::fstream map_output_file; // Mapper's intermediate output filestream
        std::atomic_bool isProducerDone; // Indicates if the producer worker is done reading the file
        std::vector<std::thread> mp_threads; // Mapper workers
        std::condition_variable buff_full; // Signals if the buffer has an item for a mapper to process
        std::condition_variable buff_empty; // Signals if the buffer is empty for the producer to put an item into it
        std::mutex buff_mutex; // Mutex over the buffer
        std::mutex map_output_file_mutex; // Mutex over the intermediate mapper output file
        bool produced; // Indicates if the producer has produced a new item into the buffer
        std::string buff; // Item buffer


        /** @brief Mapper worker routine */
        void consumer();

        /** @brief File reader worker routine */
        void producer();
        
        /** @brief Sorts the mapper output file for the reduce phase */
        void sortOutputFile();

        /** @brief Fetches a new line from the buffer */
        bool get(std::string& input_buff);

        /** @brief Puts a new line into the buffer */
        void put(std::string& input_line);

    public:

        using key_type = key; // Alias to key for public access
        using value_type = value; // Alias to value for public access

        /** @brief Constructor for the Mapper
         * @param _input_filename Relative or absolute path to the file where the mapper needs to draw the input from
         * @param _map_output_filename Relative or absolute path to the file where the mapper will store its output for the reduce phase
         * @param _MAP The mapper function
         * @param _num_mappers Number of mappers to run concurrently. Set to 1 by default, for no concurrency
         * @param _verbose Set true if you want the a verbose output. Useful for debugging
         */
        Mapper(
            const std::string& _input_filename,
            const std::string& _map_output_filename,
            const std::function<std::pair<key_type, value>(const std::string&)>& _MAP,
            const int _num_mappers = DEFAULT_NUM_MAPPERS,
            const bool _verbose = false
        );

        /** @brief Routine to run the Mapper instance */
        void run();
    };
    
    // ----- Definitions ------
    
    template<typename key, typename value>
    Mapper<key, value>::Mapper(
        const std::string& _input_filename,
        const std::string& _map_output_filename,
        const std::function<std::pair<key, value>(const std::string&)>& _MAP,
        const int _num_mappers,
        const bool _verbose
    ):  input_filename(_input_filename),
        map_output_filename(_map_output_filename), 
        MAP(_MAP), 
        num_mappers(_num_mappers),
        verbose(_verbose),
        isProducerDone(false),
        mp_threads(std::vector<std::thread>(_num_mappers)) {}

    template<typename key, typename value>
    void Mapper<key, value>::run() {
        if (verbose) std::cout << "Starting map phase..." << std::endl;

        // Initialize variables
        isProducerDone = false;
        produced = false;
        map_output_file.open(map_output_filename, std::ios::out);
        if(!map_output_file.is_open())
            throw std::runtime_error("Cannot open mapper output file: " + map_output_filename);

        // Initialize the consumers
        if (verbose) std::cout << "Starting mappers..." << std::endl;
        for (auto i = 0; i < num_mappers; i++)
            mp_threads[i] = std::thread(&Mapper<key, value>::consumer, this);
        
        // Start the producer
        std::thread producer_thread(&Mapper<key, value>::producer, this);        
        if (verbose) std::cout << "Mappers executing..." << std::endl;
        
        // Wait for threads to finish
        producer_thread.join();
        for(auto &consumer_thread: mp_threads)
            consumer_thread.join();
        if (verbose) std::cout << "Mappers execution complete successfully!" << std::endl;
        
        // Close output file
        map_output_file.close();

        // Group the values in the map_output_file by sorting it
        if (verbose) std::cout << "Grouping values by mapping keys..." << std::endl;
        sortOutputFile();
        if (verbose) std::cout << "Grouping completed successfully!" << std::endl;

        // Mapper completed successfully
        if (verbose) std::cout << "Map phase completed successfully!" << std::endl;
    }

    template<typename key, typename value>
    void Mapper<key, value>::producer() {
        // Open the input file
        std::fstream input_file;
        input_file.open(input_filename, std::ios::in);
        if(!input_file.is_open())
            throw std::runtime_error("Cannot open mapper input file: " + input_filename);

        // Producer starts writing here
        std::string input_line;
        while(std::getline(input_file,input_line))
            // Put the new line into the buffer
            put(input_line);

        // Mark producer done
        isProducerDone = true;
        
        // Must tell all sleeping consumers that the producer is done
        buff_full.notify_all();

        // Close input file
        input_file.close();
    }

    template<typename key, typename value>
    void Mapper<key, value>::consumer() {
        // Consumer repeats till the producer is done
        std::string input_line;
        while(!isProducerDone || produced) {
            // Fetch a new input line 
            if(!get(input_line))
                break;

            // Process new line
            auto new_map_pair = MAP(std::ref(input_line));

            // Write emitted value to file
            std::scoped_lock file_lock{map_output_file_mutex};
            map_output_file << new_map_pair.first << " " << new_map_pair.second << std::endl;
        }   
    }

    template<typename key, typename value>
    void Mapper<key, value>::sortOutputFile() {
        // Sort in a separate forked off process
        int wstatus;
        if(fork() == 0) {
            auto status = execlp("sort", "sort", map_output_filename.c_str(), "-o", map_output_filename.c_str(), (char*)NULL);
            std::cerr << "Mapper failed at grouping output file. Exited with status code " << status << std::endl;
        } else {
            // Wait foor the child sorting process to finish
            wait(&wstatus);

            // Sorting failed. Throw an exception
            if(!WIFEXITED(wstatus))
                throw std::runtime_error("Mapper failed at grouping output file. Mapper failed with status code " + std::to_string(wstatus));
        }
    }

    template<typename key, typename value>
    bool Mapper<key, value>::get(std::string& input_buff) {
         // Wait for a new item to process
        std::unique_lock consumer_lock{buff_mutex}; 
        buff_full.wait(consumer_lock, [&]() { return produced || isProducerDone; });
        if (!produced)
            return false;
        input_buff = std::move(buff);
        produced = false;  
        consumer_lock.unlock();
        buff_empty.notify_one();
        return true;
    }

    template<typename key, typename value>
    void Mapper<key, value>::put(std::string& input_line) {
        // Main producer logic
        std::unique_lock producer_lock{buff_mutex};
        buff_empty.wait(producer_lock, [&]() { return !produced; });
        buff = std::move(input_line);
        produced = true;
        producer_lock.unlock();
        buff_full.notify_one();
    }
}
