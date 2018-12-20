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



using namespace blazingdb::protocol;
using result_pair = std::pair<Status, std::shared_ptr<flatbuffers::DetachedBuffer>>;

static result_pair  registerFileSystem(uint64_t accessToken, Buffer&& buffer)  {
  try {
    blazingdb::protocol::UnixSocketConnection ral_client_connection{"/tmp/ral.socket"};
    interpreter::InterpreterClient ral_client{ral_client_connection};
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
    blazingdb::protocol::UnixSocketConnection ral_client_connection{"/tmp/ral.socket"};
    interpreter::InterpreterClient ral_client{ral_client_connection};
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
    blazingdb::protocol::UnixSocketConnection ral_client_connection{"/tmp/ral.socket"};
    interpreter::InterpreterClient ral_client{ral_client_connection};
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
    blazingdb::protocol::UnixSocketConnection ral_client_connection{"/tmp/ral.socket"};
    interpreter::InterpreterClient ral_client{ral_client_connection};
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
  blazingdb::protocol::UnixSocketConnection ral_client_connection{"/tmp/ral.socket"};
  interpreter::InterpreterClient ral_client{ral_client_connection};
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
    blazingdb::protocol::UnixSocketConnection calcite_client_connection{"/tmp/calcite.socket"};
    calcite::CalciteClient calcite_client{calcite_client_connection};
    auto response = calcite_client.runQuery(query);
    auto logicalPlan = response.getLogicalPlan();
    auto time = response.getTime();
    std::cout << "plan:" << logicalPlan << std::endl;
    std::cout << "time:" << time << std::endl;
    try {
      blazingdb::protocol::UnixSocketConnection ral_client_connection{"/tmp/ral.socket"};
      interpreter::InterpreterClient ral_client{ral_client_connection};

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
    blazingdb::protocol::UnixSocketConnection calcite_client_connection{"/tmp/calcite.socket"};
    calcite::CalciteClient calcite_client{calcite_client_connection};
    auto response = calcite_client.runQuery(query);
    auto logicalPlan = response.getLogicalPlan();
    auto time = response.getTime();
    std::cout << "plan:" << logicalPlan << std::endl;
    std::cout << "time:" << time << std::endl;
    try {
      blazingdb::protocol::UnixSocketConnection ral_client_connection{"/tmp/ral.socket"};
      interpreter::InterpreterClient ral_client{ral_client_connection};

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


static result_pair ddlCreateTableService(blazingdb::protocol::UnixSocketConnection &calcite_client_connection, uint64_t accessToken, Buffer&& buffer)  {
  std::cout << "###DDL Create Table: " << std::endl;
   try {
    calcite::CalciteClient calcite_client{calcite_client_connection};

    orchestrator::DDLCreateTableRequestMessage payload(buffer.data());
    std::cout << "bdname:" << payload.dbName << std::endl;
    std::cout << "table:" << payload.name << std::endl;
    for (auto col : payload.columnNames)
      std::cout << "\ntable.column:" << col << std::endl;
    
    auto status = calcite_client.createTable(  payload );
  } catch (std::runtime_error &error) {
     // error with ddl query
     std::cout << error.what() << std::endl;
     ResponseErrorMessage errorMessage{ std::string{error.what()} };
     return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  ZeroMessage response{};
  return std::make_pair(Status_Success, response.getBufferData());
};


static result_pair ddlDropTableService(blazingdb::protocol::UnixSocketConnection &calcite_client_connection, uint64_t accessToken, Buffer&& buffer)  {
  std::cout << "##DDL Drop Table: " << std::endl;
  
  try {
    calcite::CalciteClient calcite_client{calcite_client_connection};

    orchestrator::DDLDropTableRequestMessage payload(buffer.data());
    std::cout << "cbname:" << payload.dbName << std::endl;
    std::cout << "table.name:" << payload.name << std::endl;

    auto status = calcite_client.dropTable(  payload );
  } catch (std::runtime_error &error) {
    // error with ddl query
    std::cout << error.what() << std::endl;
    ResponseErrorMessage errorMessage{ std::string{error.what()} };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  ZeroMessage response{};
  return std::make_pair(Status_Success, response.getBufferData());
};
#include <thread>


int main() {
 
    blazingdb::protocol::UnixSocketConnection calcite_client_connection{"/tmp/calcite.socket"};
    const int SLEEP_TIME{1000};  //!  Milliseconds.

    for(size_t i = 0; i < 10000; i++) {
        std::cout << "##iter: " << i << std::endl;
        orchestrator::DDLCreateTableRequestMessage create_message{"nation", "main", {}, {}};
        ddlCreateTableService(calcite_client_connection, 1, Buffer {create_message.getBufferData()});
        // std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME));

        orchestrator::DDLDropTableRequestMessage drop_message{"nation", "main"};
        ddlDropTableService(calcite_client_connection, 1, Buffer {drop_message.getBufferData()});
        // std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME));

    }
  return 0;
}
