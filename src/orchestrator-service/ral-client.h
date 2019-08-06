#pragma once

#include <iostream>
#include <string>
#include <exception>

#include <blazingdb/protocol/api.h>
#include "flatbuffers/flatbuffers.h"

#include <blazingdb/protocol/message/interpreter/messages.h>
#include <blazingdb/protocol/message/io/file_system.h>
#include <blazingdb/protocol/message/orchestrator/messages.h>

namespace blazingdb {
namespace protocol {
namespace interpreter {

class InterpreterClient {
public:

    InterpreterClient(const ConnectionAddress &ralConnectionAddress)
    : client(ralConnectionAddress)
    { }

  ExecutePlanResponseMessage executeFSDirectPlan(std::string logicalPlan,
                    blazingdb::message::io::FileSystemTableGroupSchema& tableGroup,
                    blazingdb::message::io::CommunicationContextSchema& context,
                    int64_t                                access_token,
                    uint64_t resultToken) {

    blazingdb::message::io::FileSystemDMLRequestMessage message{logicalPlan, tableGroup, context, resultToken};

    auto bufferedData =
        MakeRequest(interpreter::MessageType_ExecutePlanFileSystem,
                    access_token,
                    message);


    Buffer          responseBuffer = client.send(bufferedData);
    ResponseMessage response{responseBuffer.data()};

    if (response.getStatus() == Status_Error) {
      ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
      throw std::runtime_error(errorMessage.getMessage());
    }
    ExecutePlanResponseMessage responsePayload(response.getPayloadBuffer());
    return responsePayload;
  }

  CreateTableResponseMessage parseSchema( Buffer& buffer, int64_t access_token) {
    auto bufferedData = MakeRequest(interpreter::MessageType_LoadCsvSchema,  // here I am using LoadCsvSchema but this funcion now is for parsing either CSV or Parquet
                                     access_token,
                                     buffer
                                     );

    Buffer responseBuffer = client.send(bufferedData);
    ResponseMessage response{responseBuffer.data()};

    if (response.getStatus() == Status_Error) {
      ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
      throw std::runtime_error(errorMessage.getMessage());
    }
    CreateTableResponseMessage responsePayload(response.getPayloadBuffer());
    return responsePayload;
  }


  blazingdb::protocol::BlazingTableSchema registerDaskSlice(blazingdb::protocol::BlazingTableSchema gdf, uint64_t resultToken, int64_t access_token){
	   BlazingTableSchema schema {blazingdb::protocol::BlazingTableSchema gdf, uint64_t resultToken};

	   interpreter::BlazingTableShema message;
	  auto bufferedData = MakeRequest(interpreter::MessageType_GetResult,
	                                       access_token,
	                                       message);

	    Buffer responseBuffer = client.send(bufferedData);
	    ResponseMessage response{responseBuffer.data()};

	    if (response.getStatus() == Status_Error) {
	      ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
	      throw std::runtime_error(errorMessage.getMessage());
	    }

	    interpreter::BlazingTableSchemaMessage responsePayload(response.getPayloadBuffer());


	    return {responsePayload.columns, responsePayload.columnTokens, responsePayload.resultToken};


  }

  interpreter::GetResultResponseMessage getResult(uint64_t resultToken, int64_t access_token){
    interpreter::GetResultRequestMessage payload{resultToken};
    auto bufferedData = MakeRequest(interpreter::MessageType_GetResult,
                                     access_token,
                                     payload);

    Buffer responseBuffer = client.send(bufferedData);
    ResponseMessage response{responseBuffer.data()};

    if (response.getStatus() == Status_Error) {
      ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
      throw std::runtime_error(errorMessage.getMessage());
    }
    std::cout << "get_result_status: " << response.getStatus() << std::endl;

    interpreter::GetResultResponseMessage responsePayload(response.getPayloadBuffer());
    std::cout << "getValues: " << responsePayload.getMetadata().message << std::endl;

    return responsePayload;
  }

  Status closeConnection (int64_t access_token) {
    auto payload_buffer = Buffer{};
    auto bufferedData = MakeRequest(interpreter::MessageType_CloseConnection,
                                    access_token,
                                    payload_buffer);
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
    blazingdb::message::io::FileSystemDeregisterRequestMessage payload{authority};
    auto bufferedData = MakeRequest(interpreter::MessageType_DeregisterFileSystem,
                                    access_token,
                                    payload);
    Buffer responseBuffer = client.send(bufferedData);
    ResponseMessage response{responseBuffer.data()};
    if (response.getStatus() == Status_Error) {
      ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
      throw std::runtime_error(errorMessage.getMessage());
    }
    return response.getStatus();
  }

protected:

#ifdef USE_UNIX_SOCKETS
  blazingdb::protocol::UnixSocketConnection connection;
#else
  
#endif

  blazingdb::protocol::Client client;
};


} // interpreter
} // protocol
} // blazingdb
