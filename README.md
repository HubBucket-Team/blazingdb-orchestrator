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
 

### Verbose mode

If you want to show verbose, add these args to cmake:

```bash
CUDACXX=/usr/local/cuda-9.2/bin/nvcc cmake -DCUDA_DEFINES=-DVERBOSE -DCXX_DEFINES=-DVERBOSE ...etc...
```

## Clean 
Remember to clean build each time cpp library blazingdb-protocol/develop is updated)

```
rm -rf build/
```
 
