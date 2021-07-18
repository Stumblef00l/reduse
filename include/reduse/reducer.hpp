#ifndef REDUSE_REDUCE_HPP_
#define REDUSE_REDUCE_HPP_

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
#include <reduse/modes.hpp>
#include <reduse/stopwatch.hpp>

namespace reduse {

    const int DEFAULT_NUM_REDUCERS = 1; // Default number of reducer workers

    /** @brief Handles the reduce phase
     * @param map_key Type of the key produced during the mapping phase
     * @param map_value Type of the value produced during the mapping phase
     * @param reduce_value Type of the value to be produced in reduce phase
     * @param mode Mode of execution of the Reducer
     */
    template<typename map_key, typename map_value, typename reduce_value, const Modes mode = DEFAULT_REDUSE_MODE>
    class Reducer {
    private:

        const std::string map_output_filename; // Input file for the reduce phase (produced by map phase)
        const std::string output_filename; // Output file of the reduce phase
        const std::function<reduce_value(map_key, std::vector<map_value>&)> REDUCE; // Reducer routine
        const int num_reducers; // Number of reducer workers

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


        /** @brief Logs onto console 
         * @param statement Statement displayed
         */
        inline void log(const std::string statement);
        
        /** @brief Times an operation if the mode is set to TIMING
         * @param opDesc Description of the operation
         * @param op The operation
         */
        void timeOperation(const std::string opDesc, const std::function<void(void)>& op);

    public:

        using map_key_type = map_key; // Alias to map_key for public access
        using map_value_type = map_value; // Alias to map_value for public access
        using reduce_value_type = reduce_value; // Alias to reduce_value for public access
        const Modes reducer_mode = mode; // Alias to mode for public access

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
            const int _num_reducers = DEFAULT_NUM_REDUCERS
        );

        /** @brief Routine to run the Reducer instance */
        void run();
    };

    // ------- Definitions --------

    template<typename map_key, typename map_value, typename reduce_value, const Modes mode>
    Reducer<map_key, map_value, reduce_value, mode>::Reducer(
        const std::string _map_output_filename,
        const std::string _output_filename,
        const std::function<reduce_value(map_key, std::vector<map_value>&)> _REDUCE,
        const int _num_reducers
    ):  map_output_filename(_map_output_filename),
        output_filename(_output_filename),
        REDUCE(_REDUCE),
        num_reducers(_num_reducers),
        isProducerDone(false),
        produced(false),
        rd_threads(std::vector<std::thread>(_num_reducers)) {}

    template<typename map_key, typename map_value, typename reduce_value, const Modes mode>
    void Reducer<map_key, map_value, reduce_value, mode>::run() {
        log("Starting reduce phase...");

        // Initialize variables
        isProducerDone = false;
        produced = false;
        buff_values.clear();
        output_file.open(output_filename, std::ios::out);
        if(!output_file.is_open())
            throw std::runtime_error("Cannot open reduse output file: " + output_filename);
        
        // Initialize the consumers
        log("Starting reducers...");
        for(auto i = 0; i < num_reducers; i++)
            rd_threads[i] = std::thread(&Reducer<map_key, map_value, reduce_value>::consumer, this);
        
        // Start the producer
        std::thread producer_thread(&Reducer<map_key, map_value, reduce_value>::producer, this);
        log("Reducers executing...");

        // Wait for threads to finish
        producer_thread.join();
        for(auto &consumer_thread: rd_threads)
            consumer_thread.join();
        log("Reducers execution completed successfully!");

        // Close the output file
        output_file.close();

        // Reducer completed successfully
        log("Reduce phase completed successfully!");
    }

    template<typename map_key, typename map_value, typename reduce_value, const Modes mode>
    void Reducer<map_key, map_value, reduce_value, mode>::producer() {
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

            // Input a new object everytime
            while(input >> input_key) {
                input >> input_value;

                // If key of previous object same as new object, add the value to the list
                if(input_key == curr_key) {
                    curr_values.push_back(std::move(input_value));
                    continue;
                }

                // New key, so put the old key and set of values into the buffer
                timeOperation("Reducer producer: PUT", [&]() { put(curr_key, curr_values); });

                // Update old key and set of values with new item
                timeOperation("Reducer producer: Update Key", [&]() { curr_key = std::move(input_key); });
                curr_values.clear();
                curr_values.push_back(std::move(input_value));
            }

            // Put the last object into the buffer
            timeOperation("Reducer producer: PUT", [&]() { put(curr_key, curr_values); });
        }

        // Mark the producer done
        isProducerDone = true;

        // Signal to all sleeping threads
        buff_full.notify_all();

        // Close the input file
        input.close();
    }

    template<typename map_key, typename map_value, typename reduce_value, const Modes mode>
    void Reducer<map_key, map_value, reduce_value, mode>::consumer() {

        // Consumer variables
        map_key curr_key;
        std::vector<map_value> curr_values;

        // Run until producer is done
        while(!isProducerDone || produced) {

            // Try to fetch a new item. If no new item is found, then exit
            auto fetched = false;
            timeOperation("Reducer consumer GET", [&](){ fetched = get(curr_key, curr_values); }); 
            if(!fetched)
                break;
            
            // Apply REDUCE on the new item
            reduce_value curr_result = REDUCE(curr_key, curr_values);

            // Write the new item to the output file
            std::scoped_lock output_file_lock{output_file_mutex};
            timeOperation("Reducer consumer: Write to output file", [&]() { output_file << curr_result << std::endl; });
        }
    }

    template<typename map_key, typename map_value, typename reduce_value, const Modes mode>
    void Reducer<map_key, map_value, reduce_value, mode>::put(map_key &new_key, std::vector<map_value> &new_values) {
        // Wait for the buffer to be empty
        std::unique_lock buff_lock{buff_mutex};
        buff_empty.wait(buff_lock, [&]() { return !produced; });
        
        // Put the item into the buffer and turn produced as true
        timeOperation("Reducer put: Move new_key to buffer", [&]() { buff_key = std::move(new_key); });
        timeOperation("Reducer put: Move new_values to buffer", [&]() { buff_values = std::move(new_values); });
        produced = true;

        // Nofify one sleeping consumer of the available item
        timeOperation("Redcuer put: Buffer unlock", [&]() { buff_lock.unlock(); });
        timeOperation("Reducer put: Buffer fully single notify", [&]() { buff_full.notify_one(); });
    }

    template<typename map_key, typename map_value, typename reduce_value, const Modes mode>
    bool Reducer<map_key, map_value, reduce_value, mode>::get(map_key &new_key, std::vector<map_value> &new_values) {
        // Wait for the buffer to be full
        std::unique_lock buff_lock{buff_mutex};
        buff_full.wait(buff_lock, [&](){ return ( produced || isProducerDone ); });
        
        // If no item has been produced, that means producer is done and there is nothing left to consume
        if(!produced)
            return false;

        // Put the buffered item into the consumer's local buffer and marked produced as false
        timeOperation("Reducer get: new_key moved from buffer", [&]() { new_key = std::move(buff_key); });
        timeOperation("Reducer get: new_values moved from buffer", [&]() { new_values = std::move(buff_values); });
        produced = false;

        // Signal that the buffer is empty
        timeOperation("Reducer get: buffer unlock", [&]() { buff_lock.unlock(); });
        timeOperation("Reducer get: buffer empty single notify", [&]() { buff_empty.notify_one(); });
        return true;
    }

    template<typename map_key, typename map_value, typename reduce_value, const Modes mode>
    inline void Reducer<map_key, map_value, reduce_value, mode>::log(const std::string statement) {
        if constexpr(mode == Modes::VERBOSE || mode == reduse::Modes::TIMING) {
            std::cout << statement << "\n";
        } else { 
            ; 
        }
    }

    template<typename map_key, typename map_value, typename reduce_value, const Modes mode>
    inline void Reducer<map_key, map_value, reduce_value, mode>::timeOperation(const std::string opDesc, const std::function<void(void)>& op) {
        if constexpr(mode == Modes::TIMING) {
            Stopwatch watch = Stopwatch(opDesc);
            op();
        } else {
            op();
        }
    }
}

#endif
