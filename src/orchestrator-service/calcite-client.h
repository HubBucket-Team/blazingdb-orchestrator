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
  CalciteClient(const ConnectionAddress &calciteConnectionAddress)
      : connection(calciteConnectionAddress), client(connection) {
  }

  DMLResponseMessage runQuery(std::string query) {
    try {
      int64_t sessionToken = 0;

      DMLRequestMessage message{query};
      auto    bufferedData = MakeRequest(
          calcite::MessageType_DML, sessionToken, message);
      Buffer responseBuffer = client.send(bufferedData);
      auto   response       = MakeResponse<DMLResponseMessage>(responseBuffer);
      return response;
    } catch(std::runtime_error& error) {
        throw std::runtime_error(std::string{error.what()});
    }
  }

  Status createTable(orchestrator::DDLCreateTableRequestMessage& message){
    try {
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
    } catch(std::runtime_error& error) {
        throw std::runtime_error(std::string{error.what()});
    }
  }

  Status dropTable(orchestrator::DDLDropTableRequestMessage& payload){
    try {
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
    } catch(std::runtime_error& error) {
        throw std::runtime_error(std::string{error.what()});
    }
  }


private:
  #ifdef USE_UNIX_SOCKETS

  blazingdb::protocol::UnixSocketConnection connection;

  #else
  
  blazingdb::protocol::TCPConnection connection;
  
  #endif
  
  blazingdb::protocol::Client client;
};


}
}
}
