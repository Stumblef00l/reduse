#ifndef REDUSE_STOPWATCH_HPP_
#define REDUSE_STOPWATCH_HPP_

#ifndef _REDUSE_MAIN_
#error Do not include reduse/stopwatch.hpp directly. Only reduse/reduse.hpp includes are allowed
#endif

#include <iostream>
#include <iomanip>
#include <chrono>
#include <string>

namespace reduse {

    class Stopwatch {
        
        std::string taskName;       
        std::chrono::time_point<std::chrono::high_resolution_clock> startTime;
    
    public:
        Stopwatch(std::string& taskName): taskName(std::move(taskName)), startTime(std::chrono::high_resolution_clock::now()) {}
        
        ~Stopwatch() {    
            auto endTime = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime).count();
            std::cout << taskName << " - Time taken: " <<  duration << " us\n";
        }
    };
}

#endif