#pragma once
#include <reduse.hpp>

template<typename K, typename V>
class reduse::Mapper {

    int numMappers;

public:

    using key_type = K;
    using value_type = V;

    virtual void map() = 0;
};
