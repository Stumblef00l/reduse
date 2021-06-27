# reduse
A C++ library for easy single system MapReduce UNIX file processing.

## Installation

This package can be installed globally on your system.

1. Clone the repo.
2. Inside the repo, create a `build/` directory.
3. Inside the `build/` directory, run the following commands.

```
cmake ..
cmake --build .
sudo make install
```

Post installation, for including this library in a CMake project, add the following lines to your `CMakeLists.txt` (our example target here is called `sample_target`)

```cmake
find_package(reduse REQUIRED)
find_package(Threads REQUIRED)
target_link_libraries(sample_target reduse Threads::Threads)
```

## Usage

You only need to use one method provided by the library to perform the MapReduce - `reduse::reduse` described under the header file `<reduse/reduse.hpp>`. Given is below the method signature.

```cpp
template<typename map_key, typename map_value, typename reduce_value>
void reduse(
    const std::string input_filename,
    const std::string output_filename,
    const std::function<std::pair<map_key, map_value>(const std::string&)> &MAP, 
    const std::function<reduce_value(map_key, std::vector<map_value>&)> &REDUCE,
    const int num_mappers,
    const int num_reducers,
    const bool verbose
)
```

The template parameters are described below.

1. `map_key`: Type of the key emmitted by the MAP method.
2. `map_value` Type of the value emmitted by the MAP method.
3. `reduce_value`: Type of the value emmitted by the REDUCE method.

**NOTE**: Both `map_key` and `map_value` should have both `<<` and `>>` operators defined and `reduce_value` must have a `<<` opoerator defined.

The parameters are described below.

1. `input_filename`: Full path to the input file.
2. `output_filename`: Full path to the output file. If the file does not exist, it will be created.
3. `MAP`: The mapper method. Return type must be `pair<map_key, map_value>` and it must take a single string as an argument.
4. `REDUCE`: The reducer method. Return type must be `reduce_value` and it must take `map_key` (for the key) and a `vector<map_value>` (for the list of values with that key) as arguments.
5. `num_mappers`: Number of parallel mapper workers. This is by default set to `1`.
6. `num_reducers`: Number of  parallel reducer workers. This is by default set to `1`.
7. `verbose`: Turn this to true for a more verbose output. Useful for debugging. Set to `false` by default.


Here's an example to demonstrate this usage. 

```cpp
#include <vector>
#include <string>
#include <utility>
#include <reduse/reduse.hpp>

using namespace std;

```


