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
  static CalciteClient &instance() {
    static CalciteClient singleton;
    return singleton;
  }

  DMLResponseMessage runQuery(std::string query) {
    int64_t sessionToken = 0;

    DMLRequestMessage message{query};
    auto    bufferedData = MakeRequest(
        calcite::MessageType_DML, sessionToken, message);
    Buffer responseBuffer = client.send(bufferedData);
    auto   response       = MakeResponse<DMLResponseMessage>(responseBuffer);
    return response;
  }

  Status createTable(orchestrator::DDLCreateTableRequestMessage& message){
    int64_t sessionToken = 0;
    auto bufferedData = MakeRequest(orchestrator::MessageType_DDL_CREATE_TABLE,
                                     sessionToken,
                                     message);

    Buffer responseBuffer = client.send(bufferedData);
    if (responseBuffer.size() == 0) {
      throw std::runtime_error("Orchestrattor::CreateTable failed! responseBuffer from calcite is zero");
    }

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
    if (responseBuffer.size() == 0) {
      throw std::runtime_error("Orchestrattor::DropTable failed! responseBuffer from calcite is zero");
    }
    ResponseMessage response{responseBuffer.data()};
    if (response.getStatus() == Status_Error) {
      ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
      throw std::runtime_error(errorMessage.getMessage());
    }
    return response.getStatus();
  }


private:
  CalciteClient()
      // TODO: remove global. @see main()
      : connection("/tmp/calcite.socket"), client(connection) {
  }

  blazingdb::protocol::UnixSocketConnection connection;
  blazingdb::protocol::Client client;
};


}
}
}
