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
  
Status createTable(blazingdb::protocol::ZeroMqClient &sender, orchestrator::DDLCreateTableRequestMessage& payload){
  int64_t sessionToken = 0;
  auto bufferedData = MakeRequest(orchestrator::MessageType_DDL_CREATE_TABLE,
                                    sessionToken,
                                    payload);
  auto responseBuffer = sender.send(bufferedData);
  ResponseMessage response{responseBuffer.data()};
  if (response.getStatus() == Status_Error) {
    ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
    throw std::runtime_error(errorMessage.getMessage());
  }
  return response.getStatus();
}

Status dropTable(blazingdb::protocol::ZeroMqClient &sender, orchestrator::DDLDropTableRequestMessage& payload){
  int64_t sessionToken = 0;
  auto bufferedData = MakeRequest(orchestrator::MessageType_DDL_DROP_TABLE,
                                  sessionToken,
                                  payload);
  auto responseBuffer = sender.send(bufferedData);
  ResponseMessage response{responseBuffer.data()};
  if (response.getStatus() == Status_Error) {
    ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
    throw std::runtime_error(errorMessage.getMessage());
  }
  return response.getStatus();
}



static result_pair ddlCreateTableService(blazingdb::protocol::ZeroMqClient &sender, uint64_t accessToken, Buffer&& buffer)  {
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




static result_pair ddlDropTableService(blazingdb::protocol::ZeroMqClient &sender, uint64_t accessToken, Buffer&& buffer)  {
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
    blazingdb::protocol::ZeroMqClient sender("ipc:///tmp/calcite.socket");
    for(size_t i = 0; i < 10000; i++) {
        std::cout << "##iter: " << i << std::endl;
        orchestrator::DDLCreateTableRequestMessage create_message;
        ddlCreateTableService(sender, 1, Buffer {create_message.getBufferData()});

        orchestrator::DDLDropTableRequestMessage drop_message;
        ddlDropTableService(sender, 1, Buffer {drop_message.getBufferData()}); 
        
    }
  return 0;
}

 