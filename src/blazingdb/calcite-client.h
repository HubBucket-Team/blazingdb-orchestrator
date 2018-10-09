#pragma once 

#include <iostream>
#include <exception>

#include <blazingdb/protocol/api.h>
#include "flatbuffers/flatbuffers.h"

#include <blazingdb/protocol/calcite/messages.h>
#include <blazingdb/protocol/orchestrator/messages.h>

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

  Status createTable(orchestrator::DDLCreateTableRequestMessage& payload){


    int64_t sessionToken = 0;
    auto bufferedData = MakeRequest(orchestrator::MessageType_DDL_CREATE_TABLE,
                                     0,
                                     sessionToken,
                                     payload);

    Buffer responseBuffer = client.send(bufferedData);
    ResponseMessage response{responseBuffer.data()};
    if (response.getStatus() == Status_Error) {
      ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
      throw std::runtime_error(errorMessage.getMessage());
    }
    return response.getStatus();
  }

  Status dropTable(orchestrator::DDLDropTableRequestMessage& payload){
    int64_t sessionToken = 0;
    auto bufferedData = MakeRequest(orchestrator::MessageType_DDL_DROP_TABLE,
                                    0,
                                    sessionToken,
                                    payload);

    Buffer responseBuffer = client.send(bufferedData);
    ResponseMessage response{responseBuffer.data()};
    if (response.getStatus() == Status_Error) {
      ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
      throw std::runtime_error(errorMessage.getMessage());
    }
    return response.getStatus();
  }


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
