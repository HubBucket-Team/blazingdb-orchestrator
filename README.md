# blazingdb-orchestrator

# Dependencies
- General dependencies: https://github.com/BlazingDB/blazingdb-toolchain
- BlazingDB Protocol library: https://github.com/BlazingDB/blazingdb-protocol
- BlazingDB Communication: https://github.com/BlazingDB/blazingdb-communication

# Build

```bash
cd blazingdb-orchestrator
mkdir build
CUDACXX=/usr/local/cuda-9.2/bin/nvcc cmake -DCMAKE_BUILD_TYPE=Debug \
      -DBUILD_TESTING=ON \
      -DBLAZINGDB_DEPENDENCIES_INSTALL_DIR=/foo/blazingsql/dependencies/ \
      -DBLAZINGDB_PROTOCOL_INSTALL_DIR=/foor/blazingdb_protocol_install_dir/ \
      -DBLAZINGDB_COMMUNICATION_INSTALL_DIR=/foo/blazingdb_communication_install_dir/ \
      ..
make -j8
```

**NOTE:**
If you want to build the dependencies using the old C++ ABI, add this cmake argument:

```bash
-DCXX_OLD_ABI=ON
```
