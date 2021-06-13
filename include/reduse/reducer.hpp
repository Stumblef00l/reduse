#pragma once
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

    const int DEFAULT_NUM_REDUCERS = 1;

    template<typename map_key, typename map_value, typename reduce_value>
    class Reducer {
    private:

        const std::string map_output_filename;
        const std::string output_filename;
        const std::function<reduce_value(map_key, std::vector<map_value>&)> REDUCE;
        const int num_reducers;

        std::fstream output_file;
        map_key buff_key;
        std::vector<reduce_value> buff_values;
        std::vector<std::thread> rd_threads;
        std::atomic_bool isProducerDone;
        std::mutex buff_mutex;
        std::mutex output_file_mutex;
        std::condition_variable buff_empty;
        std::condition_variable buff_full;
        bool produced;
        
        void consumer();
        void producer();

        void put(map_key &new_key, std::vector<map_value> &new_values);
        bool get(map_key &new_key, std::vector<map_value> &new_values);

    public:

        using map_key_type = map_key;
        using map_value_type = map_value;
        using reduce_value_type = reduce_value;

        Reducer(
            const std::string _map_output_filename,
            const std::string _output_filename,
            const std::function<reduce_value(map_key, std::vector<map_value>&)> _REDUCE,
            const int _num_reducers = DEFAULT_NUM_REDUCERS
        );

        void run();
    };


    template<typename map_key, typename map_value, typename reduce_value>
    Reducer<map_key, map_value, reduce_value>::Reducer(
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

    template<typename map_key, typename map_value, typename reduce_value>
    void Reducer<map_key, map_value, reduce_value>::run() {
        std::cout << "Starting reduce phase..." << std::endl;

        isProducerDone = false;
        produced = false;
        buff_values.clear();
        output_file.open(output_filename, std::ios::out);
        if(!output_file.is_open())
            throw std::runtime_error("Cannot open reduse output file: " + output_filename);
        
        std::cout << "Starting reducers..." << std::endl;
        for(auto i = 0; i < num_reducers; i++)
            rd_threads[i] = std::thread(&Reducer<map_key, map_value, reduce_value>::consumer, this);
        
        std::thread producer_thread(&Reducer<map_key, map_value, reduce_value>::producer, this);
        std::cout << "Reducers executing..." << std::endl;

        producer_thread.join();
        for(auto &consumer_thread: rd_threads)
            consumer_thread.join();
        std::cout << "Reducers execution completed successfully!";

        output_file.close();
        std::cout << "Reduce phase completed successfully!";
    }

    template<typename map_key, typename map_value, typename reduce_value>
    void Reducer<map_key, map_value, reduce_value>::producer() {
        std::fstream input;
        input.open(map_output_file, std::ios::in);
        if(!input.is_open())
            throw std::runtime_error("Unable to open map phase output file: " + map_output_file);
        
        map_key input_key;
        map_value input_value;
        map_key curr_key; 
        std::vector<map_value> curr_values;
        
        if(input >> curr_key) {
            input >> input_value;
            curr_values.push_back(input_value);
            while(input >> input_key) {
                input >> input_value;
                if(input_key.compare(curr_key) == 0) {
                    curr_values.push_back(std::move(input_value));
                    continue;
                }
                put(curr_key, curr_values);
                curr_key = std::move(input_key);
                curr_values.clear();
                curr_values.push_back(std::move(input_value));
            }
            put(curr_key, curr_values);
        }

        isProducerDone = true;
        buff_full.notify_all();
    }

    template<typename map_key, typename map_value, typename reduce_value>
    void Reducer<map_key, map_value, reduce_value>::consumer() {
        map_key curr_key;
        std::vector<map_value> curr_values;

        while(!isProducerDone) {
            if(!get(curr_key, curr_values))
                break;
            reduce_value curr_result = REDUCE(curr_key, curr_values);
            std::scoped_lock output_file_lock{output_file_mutex};
            output_file << curr_result << std::endl;
        }
    }

    template<typename map_key, typename map_value, typename reduce_value>
    void Reducer<map_key, map_value, reduce_value>::put(map_key &new_key, std::vector<map_value> &new_values) { 
        std::unique_lock buff_lock{buff_mutex};
        buff_empty.wait(buff_lock, [&]() { return !produced });
        buff_key = std::move(new_key);
        buff_values = std::move(new_values);
        produced = true;
        buff_lock.unlock();
        buff_full.notify_one();
    }

    template<typename map_key, typename map_value, typename reduce_value>
    bool Reducer<map_key, map_value, reduce_value>::get(map_key &new_key, std::vector<map_value> &new_values) {
        std::unique_lock buff_lock{buff_mutex};
        buff_full.wait(buff_lock, [&](){ return ( produced || isProducerDone )});
        if(!produced)
            return false;
        new_key = std::move(buff_key);
        new_values = std::move(buff_values);
        produced = false;
        buff_lock.unlock();
        buff_empty.notify_one();
        return true;
    }
}
