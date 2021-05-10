#pragma once
#include <mapper.hpp>
#include <reduser.hpp>

namespace reduse {
    // Main accessible classes
    template<typename K, typename V> class Mapper;
    template<typename K, typename V> class Reduser;
    
    template<typename K, typename V>
    void run_reduse(Mapper<K, V>& mapper, Reduser<K, V>& reduser);
}
