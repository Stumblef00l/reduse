#pragma once

#ifndef _REDUSE_MAIN_
#error Do not include reduse/reducer.hpp directly. Only reduse/reduse.hpp includes are allowed
#endif

#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <condition_variable>
#include <mutex>
#include <iostream>
#include <fstream>
#include <functional>
#include <exception>

namespace reduse {

    const int DEFAULT_NUM_REDUCERS = 1; // Default number of reducer workers

    /** @brief Handles the reduce phase
     * @param map_key Type of the key produced during the mapping phase
     * @param map_value Type of the value produced during the mapping phase
     * @param reduce_value Type of the value to be produced in reduce phase
     */
    template<typename map_key, typename map_value, typename reduce_value>
    class Reducer {
    private:

        const std::string map_output_filename; // Input file for the reduce phase (produced by map phase)
        const std::string output_filename; // Output file of the reduce phase
        const std::function<reduce_value(map_key, std::vector<map_value>&)> REDUCE; // Reducer routine
        const int num_reducers; // Number of reducer workers
        const bool verbose; // Verbose output to console

        std::fstream output_file; // Reducer's output filestream
        map_key buff_key; // Item buffer for the key 
        std::vector<reduce_value> buff_values; // Item buffer for the values associated with the key
        std::atomic_bool isProducerDone; // Indicates if the producer worker is finished reading the input
        bool produced; // Indicates if the producer worker has produced a new item or not
        std::vector<std::thread> rd_threads; // List of consumer workers
        std::mutex buff_mutex; // Mutex lock over the buffer
        std::mutex output_file_mutex; // Mutex lock over the output file
        std::condition_variable buff_empty; // Signals if the buffer is empty
        std::condition_variable buff_full; // Signals if the buffer is full
        
        /** @brief Reducer worker routine */
        void consumer();

        /** @brief File reader worker routine */
        void producer();

        /** @brief Adds a new item into the buffer
         * @param new_key key of the new item
         * @param new_values List of values corresponding to the item
         */
        void put(map_key &new_key, std::vector<map_value> &new_values);


        /** @brief Fetches a new item into the buffer. Returns true if some item is fetched otherwise false
         * @param new_key Location to put the key of the new item
         * @param new_values Location to put the list of values of the new item
         */
        bool get(map_key &new_key, std::vector<map_value> &new_values);

    public:

        using map_key_type = map_key; // Alias to map_key for public access
        using map_value_type = map_value; // Alias to map_value for public access
        using reduce_value_type = reduce_value; // Alias to reduce_value for public access

        /** @brief Constructor for the Reducer object 
         * @param _map_output_filename Filename for the output file produced by the mapper phase
         * @param _output_filename Filename for the output file to be produced by the reducer phase
         * @param _REDUCE The reducer function
         * @param _num_reducers Number of parallel reducer workers. Set to 1 by default, for no concurrency
        */
        Reducer(
            const std::string _map_output_filename,
            const std::string _output_filename,
            const std::function<reduce_value(map_key, std::vector<map_value>&)> _REDUCE,
            const int _num_reducers = DEFAULT_NUM_REDUCERS,
            const bool _verbose = false
        );

        /** @brief Routine to run the Reducer instance */
        void run();
    };

    // ------- Definitions --------

    template<typename map_key, typename map_value, typename reduce_value>
    Reducer<map_key, map_value, reduce_value>::Reducer(
        const std::string _map_output_filename,
        const std::string _output_filename,
        const std::function<reduce_value(map_key, std::vector<map_value>&)> _REDUCE,
        const int _num_reducers,
        const bool _verbose
    ):  map_output_filename(_map_output_filename),
        output_filename(_output_filename),
        REDUCE(_REDUCE),
        num_reducers(_num_reducers),
        verbose(_verbose),
        isProducerDone(false),
        produced(false),
        rd_threads(std::vector<std::thread>(_num_reducers)) {}

    template<typename map_key, typename map_value, typename reduce_value>
    void Reducer<map_key, map_value, reduce_value>::run() {
        if (verbose) std::cout << "Starting reduce phase..." << std::endl;

        // Initialize variables
        isProducerDone = false;
        produced = false;
        buff_values.clear();
        output_file.open(output_filename, std::ios::out);
        if(!output_file.is_open())
            throw std::runtime_error("Cannot open reduse output file: " + output_filename);
        
        // Initialize the consumers
        if (verbose) std::cout << "Starting reducers..." << std::endl;
        for(auto i = 0; i < num_reducers; i++)
            rd_threads[i] = std::thread(&Reducer<map_key, map_value, reduce_value>::consumer, this);
        
        // Start the producer
        std::thread producer_thread(&Reducer<map_key, map_value, reduce_value>::producer, this);
        if (verbose) std::cout << "Reducers executing..." << std::endl;

        // Wait for threads to finish
        producer_thread.join();
        for(auto &consumer_thread: rd_threads)
            consumer_thread.join();
        if (verbose) std::cout << "Reducers execution completed successfully!";

        // Close the output file
        output_file.close();

        // Reducer completed successfully
        if (verbose) std::cout << "Reduce phase completed successfully!";
    }

    template<typename map_key, typename map_value, typename reduce_value>
    void Reducer<map_key, map_value, reduce_value>::producer() {
        // Open the input file
        std::fstream input;
        input.open(map_output_filename, std::ios::in);
        if(!input.is_open())
            throw std::runtime_error("Unable to open map phase output file: " + map_output_filename);
        
        // Producer local variables
        map_key input_key;
        map_value input_value;
        map_key curr_key; 
        std::vector<map_value> curr_values;

        // Input the initial item. Continue producer only if there exists an initial item
        if(input >> curr_key) {
            input >> input_value;
            curr_values.push_back(input_value);

            // Input a new object everytime
            while(input >> input_key) {
                input >> input_value;

                // If key of previous object same as new object, add the value to the list
                if(input_key == curr_key) {
                    curr_values.push_back(std::move(input_value));
                    continue;
                }

                // New key, so put the old key and set of values into the buffer
                put(curr_key, curr_values);

                // Update old key and set of values with new item
                curr_key = std::move(input_key);
                curr_values.clear();
                curr_values.push_back(std::move(input_value));
            }

            // Put the last object into the buffer
            put(curr_key, curr_values);
        }

        // Mark the producer done
        isProducerDone = true;

        // Signal to all sleeping threads
        buff_full.notify_all();

        // Close the input file
        input.close();
    }

    template<typename map_key, typename map_value, typename reduce_value>
    void Reducer<map_key, map_value, reduce_value>::consumer() {

        // Consumer variables
        map_key curr_key;
        std::vector<map_value> curr_values;

        // Run until producer is done
        while(!isProducerDone || produced) {

            // Try to fetch a new item. If no new item is found, then exit
            if(!get(curr_key, curr_values))
                break;
            
            // Apply REDUCE on the new item
            reduce_value curr_result = REDUCE(curr_key, curr_values);

            // Write the new item to the output file
            std::scoped_lock output_file_lock{output_file_mutex};
            output_file << curr_result << std::endl;
        }
    }

    template<typename map_key, typename map_value, typename reduce_value>
    void Reducer<map_key, map_value, reduce_value>::put(map_key &new_key, std::vector<map_value> &new_values) {
        // Wait for the buffer to be empty
        std::unique_lock buff_lock{buff_mutex};
        buff_empty.wait(buff_lock, [&]() { return !produced; });
        
        // Put the item into the buffer and turn produced as true
        buff_key = std::move(new_key);
        buff_values = std::move(new_values);
        produced = true;

        // Nofify one sleeping consumer of the available item
        buff_lock.unlock();
        buff_full.notify_one();
    }

    template<typename map_key, typename map_value, typename reduce_value>
    bool Reducer<map_key, map_value, reduce_value>::get(map_key &new_key, std::vector<map_value> &new_values) {
        // Wait for the buffer to be full
        std::unique_lock buff_lock{buff_mutex};
        buff_full.wait(buff_lock, [&](){ return ( produced || isProducerDone ); });
        
        // If no item has been produced, that means producer is done and there is nothing left to consume
        if(!produced)
            return false;

        // Put the buffered item into the consumer's local buffer and marked produced as false
        new_key = std::move(buff_key);
        new_values = std::move(buff_values);
        produced = false;

        // Signal that the buffer is empty
        buff_lock.unlock();
        buff_empty.notify_one();
        return true;
    }
}
