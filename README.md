# blazingdb-orchestrator

## Build / Install 

Here are the steps to do so, including the necessary dependencies, just be sure to have:

- a C++11 compiler (gcc 5.5+, clang 3.8+)
- CMake 3.11+

### Install dependencies

Install Flatbuffers

```
git clone https://github.com/google/flatbuffers.git
cd flatbuffers && mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j8 install
```

## Build and Run
To run you must initialize RAl and Calcite service, see blazingdb-protocol/integration.
To use this service you could use examples from blazingdb-protocol/python/examples (develop branch)
```
cd cpp
mkdir build && cd build
cmake ..
make -j8 
./blazingdb_orchestator_service
```

## Clean 
Remember to clean build each time cpp library blazingdb-protocol/develop is updated)

```
rm -rf build/
```
 
