#pragma once

#include <iostream>
#include <string>
#include <exception>

#include <blazingdb/protocol/api.h>
#include "flatbuffers/flatbuffers.h"

#include <blazingdb/protocol/message/interpreter/messages.h>
#include <blazingdb/protocol/message/io/file_system.h>

namespace blazingdb {
namespace protocol {
namespace interpreter {

class InterpreterClient {
public:
  InterpreterClient() : client {"ipc:///tmp/ral.socket"}
  {}

  ExecutePlanResponseMessage
  executeDirectPlan(std::string                            logicalPlan,
                    const blazingdb::protocol::TableGroup *tableGroup,
                    int64_t                                access_token) {
    auto bufferedData =
        MakeRequest(interpreter::MessageType_ExecutePlan,
                    access_token,
                    ExecutePlanDirectRequestMessage{logicalPlan, tableGroup});

    Buffer          responseBuffer = client.send(bufferedData);
    ResponseMessage response{responseBuffer.data()};

    if (response.getStatus() == Status_Error) {
      ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
      throw std::runtime_error(errorMessage.getMessage());
    }
    ExecutePlanResponseMessage responsePayload(response.getPayloadBuffer());
    return responsePayload;
  }

  ExecutePlanResponseMessage executeFSDirectPlan(std::string logicalPlan,
                    blazingdb::message::io::FileSystemTableGroupSchema& tableGroup,
                    int64_t                                access_token) {
    auto bufferedData =
        MakeRequest(interpreter::MessageType_ExecutePlanFileSystem,
                    access_token,
                    blazingdb::message::io::FileSystemDMLRequestMessage{logicalPlan, tableGroup});


    Buffer          responseBuffer = client.send(bufferedData);
    ResponseMessage response{responseBuffer.data()};

    if (response.getStatus() == Status_Error) {
      ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
      throw std::runtime_error(errorMessage.getMessage());
    }
    ExecutePlanResponseMessage responsePayload(response.getPayloadBuffer());
    return responsePayload;
  }

  std::shared_ptr<flatbuffers::DetachedBuffer> executePlan(std::string logicalPlan, const ::blazingdb::protocol::TableGroupDTO &tableGroup, int64_t access_token)  {
    auto bufferedData = MakeRequest(interpreter::MessageType_ExecutePlan,
                                     access_token,
                                     ExecutePlanRequestMessage{logicalPlan, tableGroup});

    Buffer responseBuffer = client.send(bufferedData);
    ResponseMessage response{responseBuffer.data()};

    if (response.getStatus() == Status_Error) {
      ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
      throw std::runtime_error(errorMessage.getMessage());
    }
    ExecutePlanResponseMessage responsePayload(response.getPayloadBuffer());
    return responsePayload.getBufferData();
  }

  std::shared_ptr<flatbuffers::DetachedBuffer> loadCsvSchema( Buffer& buffer, int64_t access_token) {
    auto bufferedData = MakeRequest(interpreter::MessageType_LoadCsvSchema,
                                     access_token,
                                     buffer
                                     );

    Buffer responseBuffer = client.send(bufferedData);
    ResponseMessage response{responseBuffer.data()};

    if (response.getStatus() == Status_Error) {
      ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
      throw std::runtime_error(errorMessage.getMessage());
    }
    ExecutePlanResponseMessage responsePayload(response.getPayloadBuffer());
    return responsePayload.getBufferData();
  }

  std::shared_ptr<flatbuffers::DetachedBuffer> loadParquetSchema( Buffer& buffer, int64_t access_token) {
    auto bufferedData = MakeRequest(interpreter::MessageType_LoadParquetSchema,
                                     access_token,
                                     buffer
                                     );

    Buffer responseBuffer = client.send(bufferedData);
    ResponseMessage response{responseBuffer.data()};

    if (response.getStatus() == Status_Error) {
      ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
      throw std::runtime_error(errorMessage.getMessage());
    }
    ExecutePlanResponseMessage responsePayload(response.getPayloadBuffer());
    return responsePayload.getBufferData();
  }

  std::vector<::gdf_dto::gdf_column> getResult(uint64_t resultToken, int64_t access_token){
    auto bufferedData = MakeRequest(interpreter::MessageType_GetResult,
                                     access_token,
                                     interpreter::GetResultRequestMessage {resultToken});

    Buffer responseBuffer = client.send(bufferedData);
    ResponseMessage response{responseBuffer.data()};

    if (response.getStatus() == Status_Error) {
      ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
      throw std::runtime_error(errorMessage.getMessage());
    }
    std::cout << "get_result_status: " << response.getStatus() << std::endl;

    interpreter::GetResultResponseMessage responsePayload(response.getPayloadBuffer());
    std::cout << "getValues: " << responsePayload.getMetadata().message << std::endl;

    return responsePayload.getColumns();
  }

  Status closeConnection (int64_t access_token) {
    auto bufferedData = MakeRequest(interpreter::MessageType_CloseConnection,
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
  Status registerFileSystem(int64_t access_token, Buffer& buffer) {
    auto bufferedData = MakeRequest(interpreter::MessageType_RegisterFileSystem,
                                    access_token,
                                    buffer
                                    );
    Buffer responseBuffer = client.send(bufferedData);
    ResponseMessage response{responseBuffer.data()};
    if (response.getStatus() == Status_Error) {
      ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
      throw std::runtime_error(errorMessage.getMessage());
    }
    return response.getStatus();
  }
  
  Status deregisterFileSystem(int64_t access_token, const std::string& authority) {
    auto bufferedData = MakeRequest(interpreter::MessageType_DeregisterFileSystem,
                                    access_token,
                                    blazingdb::message::io::FileSystemDeregisterRequestMessage{authority});
    Buffer responseBuffer = client.send(bufferedData);
    ResponseMessage response{responseBuffer.data()};
    if (response.getStatus() == Status_Error) {
      ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
      throw std::runtime_error(errorMessage.getMessage());
    }
    return response.getStatus();
  }

protected:
  blazingdb::protocol::ZeroMqClient client;
};


} // interpreter
} // protocol
} // blazingdb
