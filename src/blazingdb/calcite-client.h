#pragma once 

#include <iostream>
#include <exception>

#include <blazingdb/protocol/api.h>
#include "flatbuffers/flatbuffers.h"

#include <blazingdb/protocol/calcite/messages.h>

namespace blazingdb {
namespace protocol { 

namespace calcite { 

class CalciteClient {
public:
  CalciteClient(blazingdb::protocol::Connection& con) : client { con }
  {}

  //todo: reducir codigo usando MakeRequest & MakeResponse
  std::string getLogicalPlan(std::string query)  {

    int64_t sessionToken = 0;
    auto bufferedData = MakeRequest(calcite::MessageType_DML,
                                     query.length(),
                                     sessionToken,
                                     DMLRequestMessage{query});
    Buffer responseBuffer = client.send(bufferedData);
    auto response = MakeResponse<DMLResponseMessage>(responseBuffer);
    return response.getLogicalPlan();
  }

  //todo: reducir codigo usando MakeRequest & MakeResponse
  Status updateSchema(std::string query)    {

    int64_t sessionToken = 0;
    auto bufferedData = MakeRequest(calcite::MessageType_DDL,
                                     query.length(),
                                     sessionToken,
                                     DDLRequestMessage{query});

    Buffer responseBuffer = client.send(bufferedData);

    ResponseMessage response{responseBuffer.data()};
    if (response.getStatus() == Status_Error) {
      ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
      throw std::runtime_error(errorMessage.getMessage());
    }
    return response.getStatus();
  }

private:
  blazingdb::protocol::Client client;
};


}
}
}
