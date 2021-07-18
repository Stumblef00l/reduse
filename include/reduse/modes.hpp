#ifndef REDUSE_MODES_HPP_
#define REDUSE_MODES_HPP_

namespace reduse {
    
    enum class Modes {
        QUIET,
        VERBOSE,
        TIMING
    };

    const Modes DEFAULT_REDUSE_MODE = Modes::QUIET;
}

#endif