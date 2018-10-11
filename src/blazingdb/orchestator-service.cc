#include <iostream>
#include <map>
#include <tuple>
#include <blazingdb/protocol/api.h>

#include "ral-client.h"
#include "calcite-client.h"
#include <blazingdb/protocol/orchestrator/messages.h>


using namespace blazingdb::protocol;
using result_pair = std::pair<Status, std::shared_ptr<flatbuffers::DetachedBuffer>>;

static result_pair  openConnectionService(uint64_t nonAccessToken, Buffer&& buffer)  {
  int64_t token = 123456789L; // get_uuid()
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




static result_pair  dmlService(uint64_t accessToken, Buffer&& buffer)  {
  orchestrator::DMLRequestMessage requestPayload(buffer.data());
  auto query = requestPayload.getQuery();
  std::cout << "DML: " << query << std::endl;
  uint64_t resultToken = 0L;

  try {
    blazingdb::protocol::UnixSocketConnection calcite_client_connection{"/tmp/calcite.socket"};
    calcite::CalciteClient calcite_client{calcite_client_connection};
    auto logicalPlan = calcite_client.getLogicalPlan(query);
    std::cout << "plan:" << logicalPlan << std::endl;
    try {
      blazingdb::protocol::UnixSocketConnection ral_client_connection{"/tmp/ral.socket"};
      interpreter::InterpreterClient ral_client{ral_client_connection};
      resultToken = ral_client.executePlan(logicalPlan, requestPayload.getTableGroup(), accessToken);
      std::cout << "resultToken:" << resultToken << std::endl;
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
  orchestrator::DMLResponseMessage response{resultToken};
  return std::make_pair(Status_Success, response.getBufferData());
};
 
 
static result_pair ddlCreateTableService(uint64_t accessToken, Buffer&& buffer)  {
  std::cout << "DDL Create Table: " << std::endl;
   try {
    blazingdb::protocol::UnixSocketConnection calcite_client_connection{"/tmp/calcite.socket"};
    calcite::CalciteClient calcite_client{calcite_client_connection};

    orchestrator::DDLCreateTableRequestMessage payload(buffer.data()); 
    auto status = calcite_client.createTable(  payload );
    std::cout << "status:" << status << std::endl;
  } catch (std::runtime_error &error) {
     // error with ddl query
     std::cout << error.what() << std::endl;
     ResponseErrorMessage errorMessage{ std::string{error.what()} };
     return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  ZeroMessage response{};
  return std::make_pair(Status_Success, response.getBufferData());
};


static result_pair ddlDropTableService(uint64_t accessToken, Buffer&& buffer)  {
  std::cout << "DDL Drop Table: " << std::endl;
  try {
    blazingdb::protocol::UnixSocketConnection calcite_client_connection{"/tmp/calcite.socket"};
    calcite::CalciteClient calcite_client{calcite_client_connection};

    orchestrator::DDLDropTableRequestMessage payload(buffer.data());
    auto status = calcite_client.dropTable(  payload );
    std::cout << "status:" << status << std::endl;
  } catch (std::runtime_error &error) {
    // error with ddl query
    std::cout << error.what() << std::endl;
    ResponseErrorMessage errorMessage{ std::string{error.what()} };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  ZeroMessage response{};
  return std::make_pair(Status_Success, response.getBufferData());
};


using FunctionType = result_pair (*)(uint64_t, Buffer&&);

int main() {
  blazingdb::protocol::UnixSocketConnection server_connection({"/tmp/orchestrator.socket", std::allocator<char>()});
  blazingdb::protocol::Server server(server_connection);


  std::map<int8_t, FunctionType> services;
  services.insert(std::make_pair(orchestrator::MessageType_DML, &dmlService));

  services.insert(std::make_pair(orchestrator::MessageType_DDL_CREATE_TABLE, &ddlCreateTableService));
  services.insert(std::make_pair(orchestrator::MessageType_DDL_DROP_TABLE, &ddlDropTableService));

  services.insert(std::make_pair(orchestrator::MessageType_AuthOpen, &openConnectionService));
  services.insert(std::make_pair(orchestrator::MessageType_AuthClose, &closeConnectionService));

  auto orchestratorService = [&services](const blazingdb::protocol::Buffer &requestBuffer) -> blazingdb::protocol::Buffer {
    RequestMessage request{requestBuffer.data()};
    std::cout << "header: " << (int)request.messageType() << std::endl;

    auto result = services[request.messageType()] ( request.accessToken(),  request.getPayloadBuffer() );
    ResponseMessage responseObject{result.first, result.second};
    auto bufferedData = responseObject.getBufferData();
    Buffer buffer{bufferedData->data(),
                  bufferedData->size()};
    return buffer;
  };
  server.handle(orchestratorService);
  return 0;
}

