#include <iostream>
#include <map>
#include <tuple>
#include <blazingdb/protocol/api.h>

#include "ral-client.h"
#include "calcite-client.h"

#include <blazingdb/protocol/message/interpreter/messages.h>
#include <blazingdb/protocol/message/messages.h>
#include <blazingdb/protocol/message/orchestrator/messages.h>
#include <blazingdb/protocol/message/io/file_system.h>

#include <cstdlib>     /* srand, rand */
#include <ctime>       /* time */

#include "zmq/zmq.hpp"

using namespace blazingdb::protocol;
using result_pair = std::pair<Status, std::shared_ptr<flatbuffers::DetachedBuffer>>;

static result_pair  registerFileSystem(uint64_t accessToken, Buffer&& buffer)  {
  try {
    interpreter::InterpreterClient ral_client{};
    auto response = ral_client.registerFileSystem(accessToken, buffer);

  } catch (std::runtime_error &error) {
    // error with query plan: not resultToken
    std::cout << error.what() << std::endl;
    ResponseErrorMessage errorMessage{ std::string{error.what()} };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  ZeroMessage response{};
  return std::make_pair(Status_Success, response.getBufferData());
}

static result_pair  deregisterFileSystem(uint64_t accessToken, Buffer&& buffer)  {
  try {
    interpreter::InterpreterClient ral_client{};
    blazingdb::message::io::FileSystemDeregisterRequestMessage message(buffer.data());
    auto response = ral_client.deregisterFileSystem(accessToken, message.getAuthority());

  } catch (std::runtime_error &error) {
    // error with query plan: not resultToken
    std::cout << error.what() << std::endl;
    ResponseErrorMessage errorMessage{ std::string{error.what()} };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  ZeroMessage response{};
  return std::make_pair(Status_Success, response.getBufferData());
}

static result_pair loadCsvSchema(uint64_t accessToken, Buffer&& buffer) {
  std::shared_ptr<flatbuffers::DetachedBuffer> resultBuffer;
   try {
    interpreter::InterpreterClient ral_client{};
    resultBuffer = ral_client.loadCsvSchema(buffer, accessToken);

  } catch (std::runtime_error &error) {
    // error with query plan: not resultToken
    std::cout << error.what() << std::endl;
    ResponseErrorMessage errorMessage{ std::string{error.what()} };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  return std::make_pair(Status_Success, resultBuffer);
}


static result_pair loadParquetSchema(uint64_t accessToken, Buffer&& buffer) {
  std::shared_ptr<flatbuffers::DetachedBuffer> resultBuffer;
   try {
    interpreter::InterpreterClient ral_client{};
    resultBuffer = ral_client.loadParquetSchema(buffer, accessToken);

  } catch (std::runtime_error &error) {
    // error with query plan: not resultToken
    std::cout << error.what() << std::endl;
    ResponseErrorMessage errorMessage{ std::string{error.what()} };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  return std::make_pair(Status_Success, resultBuffer);
}


static result_pair  openConnectionService(uint64_t nonAccessToken, Buffer&& buffer)  {
  srand(time(0));
  int64_t token = rand();
  orchestrator::AuthResponseMessage response{token};
  std::cout << "authorizationService: " << token << std::endl;
  return std::make_pair(Status_Success, response.getBufferData());
};


static result_pair  closeConnectionService(uint64_t accessToken, Buffer&& buffer)  {
  interpreter::InterpreterClient ral_client{};
  try {
    auto status = ral_client.closeConnection(accessToken);
    std::cout << "status:" << status << std::endl;
  } catch (std::runtime_error &error) {
    ResponseErrorMessage errorMessage{ std::string{error.what()} };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  ZeroMessage response{};
  return std::make_pair(Status_Success, response.getBufferData());
};

static result_pair  dmlFileSystemService (uint64_t accessToken, Buffer&& buffer) {
  blazingdb::message::io::FileSystemDMLRequestMessage requestPayload(buffer.data());
  auto query = requestPayload.statement;
  std::cout << "##DML-FS: " << query << std::endl;
  std::shared_ptr<flatbuffers::DetachedBuffer> resultBuffer;

  try {
    calcite::CalciteClient calcite_client{};
    auto response = calcite_client.runQuery(query);
    auto logicalPlan = response.getLogicalPlan();
    auto time = response.getTime();
    std::cout << "plan:" << logicalPlan << std::endl;
    std::cout << "time:" << time << std::endl;
    try {
      interpreter::InterpreterClient ral_client{};

      auto executePlanResponseMessage = ral_client.executeFSDirectPlan(logicalPlan, requestPayload.tableGroup, accessToken);
      
      auto nodeInfo = executePlanResponseMessage.getNodeInfo();
      auto dmlResponseMessage = orchestrator::DMLResponseMessage(
          executePlanResponseMessage.getResultToken(),
          nodeInfo, time);
      resultBuffer = dmlResponseMessage.getBufferData();
    } catch (std::runtime_error &error) {
      // error with query plan: not resultToken
      std::cout << error.what() << std::endl;
      ResponseErrorMessage errorMessage{ std::string{error.what()} };
      return std::make_pair(Status_Error, errorMessage.getBufferData());
    }
  } catch (std::runtime_error &error) {
    // error with query: not logical plan error
    std::cout << error.what() << std::endl;
    ResponseErrorMessage errorMessage{ std::string{error.what()} };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  return std::make_pair(Status_Success, resultBuffer);
}

static result_pair  dmlService(uint64_t accessToken, Buffer&& buffer)  {
  orchestrator::DMLRequestMessage requestPayload(buffer.data());
  auto query = requestPayload.getQuery();
  std::cout << "DML: " << query << std::endl;
  std::shared_ptr<flatbuffers::DetachedBuffer> resultBuffer;

  try {
    calcite::CalciteClient calcite_client;
    auto response = calcite_client.runQuery(query);
    auto logicalPlan = response.getLogicalPlan();
    auto time = response.getTime();
    std::cout << "plan:" << logicalPlan << std::endl;
    std::cout << "time:" << time << std::endl;
    try {
      interpreter::InterpreterClient ral_client;

      auto executePlanResponseMessage = ral_client.executeDirectPlan(
          logicalPlan, requestPayload.getTableGroup(), accessToken);
      auto nodeInfo = executePlanResponseMessage.getNodeInfo();
      auto dmlResponseMessage = orchestrator::DMLResponseMessage(
          executePlanResponseMessage.getResultToken(),
          nodeInfo, time);
      resultBuffer = dmlResponseMessage.getBufferData();
    } catch (std::runtime_error &error) {
      // error with query plan: not resultToken
      std::cout << error.what() << std::endl;
      ResponseErrorMessage errorMessage{ std::string{error.what()} };
      return std::make_pair(Status_Error, errorMessage.getBufferData());
    }
  } catch (std::runtime_error &error) {
    // error with query: not logical plan error
    std::cout << error.what() << std::endl;
    ResponseErrorMessage errorMessage{ std::string{error.what()} };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  return std::make_pair(Status_Success, resultBuffer);
};



Status createTable(zmq::socket_t &sender, orchestrator::DDLCreateTableRequestMessage& payload){
  int64_t sessionToken = 0;
  auto bufferedData = MakeRequest(orchestrator::MessageType_DDL_CREATE_TABLE,
                                    sessionToken,
                                    payload);

  
  assert(bufferedData->size() == sender.send(bufferedData->data(), bufferedData->size()));
  
  zmq::message_t request_message;
  sender.recv(&request_message);
  Buffer responseBuffer((uint8_t*)request_message.data(), request_message.size());

  ResponseMessage response{responseBuffer.data()};
  if (response.getStatus() == Status_Error) {
    ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
    throw std::runtime_error(errorMessage.getMessage());
  }
  return response.getStatus();
}

Status dropTable(zmq::socket_t &sender, orchestrator::DDLDropTableRequestMessage& payload){
  int64_t sessionToken = 0;
  auto bufferedData = MakeRequest(orchestrator::MessageType_DDL_DROP_TABLE,
                                  sessionToken,
                                  payload);

  assert(bufferedData->size() == sender.send(bufferedData->data(), bufferedData->size()));
  
  zmq::message_t request_message;
  sender.recv(&request_message);
  Buffer responseBuffer((uint8_t*)request_message.data(), request_message.size());

  ResponseMessage response{responseBuffer.data()};
  if (response.getStatus() == Status_Error) {
    ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
    throw std::runtime_error(errorMessage.getMessage());
  }
  return response.getStatus();
}



static result_pair ddlCreateTableService(zmq::socket_t &sender, uint64_t accessToken, Buffer&& buffer)  {
  std::cout << "###DDL Create Table: " << std::endl;
   try {
    orchestrator::DDLCreateTableRequestMessage payload(buffer.data());
    std::cout << "bdname:" << payload.dbName << std::endl;
    std::cout << "table:" << payload.name << std::endl;
    for (auto col : payload.columnNames)
      std::cout << "\ntable.column:" << col << std::endl;
    
    auto status = createTable(sender,  payload );
    std::cout << "status:" << status  << std::endl;
  } catch (std::runtime_error &error) {
     // error with ddl query
     std::cout << error.what() << std::endl;
     ResponseErrorMessage errorMessage{ std::string{error.what()} };
     return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  ZeroMessage response{};
  return std::make_pair(Status_Success, response.getBufferData());
};




static result_pair ddlDropTableService(zmq::socket_t &sender, uint64_t accessToken, Buffer&& buffer)  {
  std::cout << "##DDL Drop Table: " << std::endl;
  
  try {
    orchestrator::DDLDropTableRequestMessage payload(buffer.data());
    std::cout << "cbname:" << payload.dbName << std::endl;
    std::cout << "table.name:" << payload.name << std::endl;

    auto status = dropTable(sender,  payload );
  } catch (std::runtime_error &error) {
    // error with ddl query
    std::cout << error.what() << std::endl;
    ResponseErrorMessage errorMessage{ std::string{error.what()} };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  ZeroMessage response{};
  return std::make_pair(Status_Success, response.getBufferData());
};

int main () {
  zmq::context_t context;
  zmq::socket_t sender(context, ZMQ_REQ);
  sender.connect("ipc:///tmp/calcite.socket");

  for(size_t i = 0; i < 10000; i++) {
      std::cout << "##iter: " << i << std::endl;
      orchestrator::DDLCreateTableRequestMessage create_message;
      ddlCreateTableService(sender, 1, Buffer {create_message.getBufferData()});

      orchestrator::DDLDropTableRequestMessage drop_message;
      ddlDropTableService(sender, 1, Buffer {drop_message.getBufferData()});
  }
  return 0;
}


// int main () {
//     zmq::context_t context;
//     zmq::socket_t sender(context, ZMQ_REQ);
//     sender.connect("ipc:///tmp/test.ipc");
//     assert(2 == sender.send("Hi", 2));
    
//     zmq::message_t request_message;
//     sender.recv(&request_message);
//     printf("done.\n");
//     printf("\t\tMessage: Size: %d ; Value: %s\n", static_cast<int>(request_message.size()), static_cast<const char*>(request_message.data()));
//     return 0;
// }