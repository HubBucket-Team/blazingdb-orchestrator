#include <iostream>
#include <string>
#include <exception>

#include <blazingdb/protocol/api.h>
#include "flatbuffers/flatbuffers.h"
 
#include "interpreter-messages.h"

namespace blazingdb {
namespace protocol { 

namespace interpreter {
 
class InterpreterClient {
public:
  InterpreterClient(blazingdb::protocol::Connection & connection) : client {connection}
  {}

  std::string executePlan(std::string logicalPlan)  {
    DMLRequestMessage requestPayload{logicalPlan};
    
    RequestMessage requestObject{interpreter::MessageType_ExecutePlan, requestPayload}; 
    
    auto bufferedData = requestObject.getBufferData();

    std::basic_string<uint8_t> oef_concat(bufferedData->data(), bufferedData->size());
    
    oef_concat += std::basic_string<uint8_t>((const uint8_t * )"\n");

    Buffer buffer{oef_concat.c_str(), 
                  oef_concat.size()};

    std::cout << "\t1.1\n";
    Buffer responseBuffer = client.send(buffer);
    std::cout << "\t1.2\n";

    ResponseMessage response{responseBuffer.data()};
    if (response.getStatus() == Status_Error) {
      ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
      throw std::runtime_error(errorMessage.getMessage());
    }
    DMLResponseMessage responsePayload(response.getPayloadBuffer());
    return responsePayload.getToken();
  }
 

private:
  blazingdb::protocol::Client client;
};

}

}
}
using namespace blazingdb::protocol::interpreter;

int main() {
  blazingdb::protocol::UnixSocketConnection connection("/tmp/socket");
  InterpreterClient client{connection};

  {
    std::string logicalPlan = "LogicalUnion(all=[false])";
    try {
      std::string token = client.executePlan(logicalPlan);
      std::cout << token << std::endl;
    } catch (std::runtime_error &error) {
      std::cout << error.what() << std::endl;
    }

    // logicalPlan = "example_error_logical_plan\n";
    // try {
    //   std::cout << "1\n";
    //   std::string token = client.executePlan(logicalPlan);
    //   std::cout << "2\n";
    //   std::cout << token << std::endl;
    // } catch (std::runtime_error &error) {
    //   std::cout << error.what() << std::endl;
    // }
  } 
  return 0;
}
