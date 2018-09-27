#include <iostream>

#include <blazingdb/protocol/api.h>

int main() {
  blazingdb::protocol::UnixSocketConnection connection(
      "/tmp/blazingdb-orchestator.socket");

  blazingdb::protocol::Server server(connection);

  server.handle([](const blazingdb::protocol::Buffer &requestBuffer)
                    -> blazingdb::protocol::Buffer {
    std::cout << requestBuffer.data() << std::endl;

    return blazingdb::protocol::Buffer(reinterpret_cast<const std::uint8_t *>(
                                           "Blazingdb Orchestator Response"),
                                       30);
  });

  return 0;
}
