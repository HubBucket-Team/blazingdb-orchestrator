#pragma once

#include <iostream>
#include <exception>

#include <blazingdb/protocol/api.h>
#include "flatbuffers/flatbuffers.h"

#include <blazingdb/protocol/message/calcite/messages.h>
#include <blazingdb/protocol/message/orchestrator/messages.h>

namespace blazingdb {
namespace protocol {

namespace calcite {

class CalciteClient {
public:
  CalciteClient() : client { "ipc:///tmp/calcite.socket" }
  {}

  DMLResponseMessage runQuery(std::string query) {
    int64_t sessionToken = 0;
    auto    bufferedData = MakeRequest(
        calcite::MessageType_DML, sessionToken, DMLRequestMessage{query});
    Buffer responseBuffer = client.send(bufferedData);
    auto   response       = MakeResponse<DMLResponseMessage>(responseBuffer);
    return response;
  }

  Status createTable(orchestrator::DDLCreateTableRequestMessage& payload){
    int64_t sessionToken = 0;
    auto bufferedData = MakeRequest(orchestrator::MessageType_DDL_CREATE_TABLE,
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
  blazingdb::protocol::ZeroMqClient client;
};


}
}
}
