#include <iostream>

#include <blazingdb/protocol/api.h>

#include "interpreter-messages.h"

namespace blazingdb {
namespace protocol { 

namespace interpreter { 

auto InterpreterService(const blazingdb::protocol::Buffer &requestBuffer) -> blazingdb::protocol::Buffer {
  RequestMessage request{requestBuffer.data()};
  DMLRequestMessage requestPayload(request.getPayloadBuffer());

  std::cout << "header: " << request.header() << std::endl;
  std::cout << "query: " << requestPayload.getLogicalPlan() << std::endl;

  std::string token = "JIFY*DSA%^F*(*(S)DIKFJLNDVOYD(";

  DMLResponseMessage responsePayload{token};
  ResponseMessage responseObject{Status_Success, responsePayload};
  auto bufferedData = responseObject.getBufferData();
  Buffer buffer{bufferedData->data(),
              bufferedData->size()};
  return buffer;
}

}
}
}

using namespace blazingdb::protocol::interpreter;

int main() {
  blazingdb::protocol::UnixSocketConnection connection({"/tmp/socket", std::allocator<char>()});
  blazingdb::protocol::Server server(connection);

 
  server.handle(InterpreterService);

  return 0;
}
