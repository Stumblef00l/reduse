#pragma once
#include <reduse.hpp>

template<typename K, typename V>
class reduse::Reduser {

public:

    using key_type = K;
    using value_type = V;

    virtual void reduse() = 0;
    virtual void balanse() = 0;
};
