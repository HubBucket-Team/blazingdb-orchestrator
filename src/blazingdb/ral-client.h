#pragma once 

#include <iostream>
#include <string>
#include <exception>

#include <blazingdb/protocol/api.h>
#include "flatbuffers/flatbuffers.h"

#include <blazingdb/protocol/interpreter/messages.h>

namespace blazingdb {
namespace protocol { 



namespace interpreter {
 
class InterpreterClient {
public:
  InterpreterClient(blazingdb::protocol::Connection & connection) : client {connection}
  {}

  uint64_t executePlan(std::string logicalPlan, int64_t access_token)  {
    auto bufferedData = MakeRequest(interpreter::MessageType_ExecutePlan,
                                     logicalPlan.length(),
                                     access_token,
                                     DMLRequestMessage{logicalPlan});

    Buffer responseBuffer = client.send(bufferedData);
    ResponseMessage response{responseBuffer.data()};

    if (response.getStatus() == Status_Error) {
      ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
      throw std::runtime_error(errorMessage.getMessage());
    }
    DMLResponseMessage responsePayload(response.getPayloadBuffer());
    return responsePayload.getToken();
  }

  Status closeConnection (int64_t access_token) {
    auto bufferedData = MakeRequest(interpreter::MessageType_CloseConnection,
                                    0,
                                    access_token,
                                    ZeroMessage{});
    Buffer responseBuffer = client.send(bufferedData);
    ResponseMessage response{responseBuffer.data()};
    if (response.getStatus() == Status_Error) {
      ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
      throw std::runtime_error(errorMessage.getMessage());
    }
    return response.getStatus();
  }

protected:
  blazingdb::protocol::Client client;
};

}


}
}