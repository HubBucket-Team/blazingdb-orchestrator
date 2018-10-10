# blazingdb-orchestrator

## Build / Install 

Here are the steps to do so, including the necessary dependencies, just be sure to have:

- a C++11 compiler (gcc 5.5+, clang 3.8+)
- CMake 3.11+

### Install dependencies

Install blazingdb-protocol 

https://github.com/BlazingDB/blazingdb-protocol/tree/develop

- cpp-library
- java-library
- python-library

## Build and Run
To run orchestrator-service you must initialize RAl and Calcite service, see blazingdb-protocol/integration.
```
cd blazingdb-protocol/integration
bash build.sh

echo "run calcite service"
cd blazingdb-protocol/integration/services/services/calcite-service
java -jar target/calcite-service-1.0-SNAPSHOT.jar 

echo "running ral service"
cd blazingdb-protocol/integration/services/cpp/build/ 
./blazingdb_ral_service 
```

Build and run orchestrator-service
```
cd cpp
mkdir build && cd build
cmake ..
make -j8 
echo "running orchestrator service"
./blazingdb_orchestator_service
```

To use this service you could use examples from blazingdb-protocol/python/examples (develop branch)
```
cd blazingdb-protocol/integration/clients/python-connector
python3 py-connector.py
python3 dml_create_table.py
```

## Clean 
Remember to clean build each time cpp library blazingdb-protocol/develop is updated)

```
rm -rf build/
```
 
