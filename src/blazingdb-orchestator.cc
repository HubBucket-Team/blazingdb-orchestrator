#include <iostream>
#include <exception>

#include <blazingdb/protocol/api.h>
#include "flatbuffers/flatbuffers.h"
 
namespace blazingdb {
namespace protocol { 
  
class CalciteClient {
public:
  CalciteClient(blazingdb::protocol::Connection & connection) : client {connection}
  {}

  std::string getLogicalPlan(std::string query)  {
    DMLRequestMessage requestPayload{query};
    RequestMessage requestObject{calcite::MessageType_DML, requestPayload}; 
    
    auto bufferedData = requestObject.getBufferData();

    Buffer buffer{bufferedData->data(), 
                  bufferedData->size()};

    Buffer responseBuffer = client.send(buffer);
    ResponseMessage response{responseBuffer.data()};
    if (response.getStatus() == Status_Error) {
      ResponseErrorMessage errorMessage{response.getPayloadBuffer()};
      throw std::runtime_error(errorMessage.getMessage());
    }
    DMLResponseMessage responsePayload(response.getPayloadBuffer());
    return responsePayload.getLogicalPlan();
  }

  Status updateSchema(std::string statement)    {
    DDLRequestMessage requestPayload{statement};
    RequestMessage requestObject{calcite::MessageType_DDL, requestPayload}; 
    
    auto bufferedData = requestObject.getBufferData();

    Buffer buffer{bufferedData->data(), 
                  bufferedData->size()};

    Buffer responseBuffer = client.send(buffer);
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
using namespace blazingdb::protocol;


int main() {
  blazingdb::protocol::UnixSocketConnection connection("/tmp/socket");
  CalciteClient client{connection};
  
  {
    std::string query = "select * from orders";
    try {
      std::string logicalPlan = client.getLogicalPlan(query);
      std::cout << logicalPlan << std::endl;
    } catch (std::runtime_error &error) {
      std::cout << error.what() << std::endl;
    }

    query = "error_dml_query_example";
    try {
      std::string logicalPlan = client.getLogicalPlan(query);
      std::cout << logicalPlan << std::endl;
    } catch (std::runtime_error &error) {
      std::cout << error.what() << std::endl;
    }
  }

  {
    std::string query = "create database alexdb";
    try {
      auto status = client.updateSchema(query);
      std::cout << status << std::endl;
    } catch (std::runtime_error &error) {
      std::cout << error.what() << std::endl;
    }

    query = "error_ddl_query_example";
    try {
      auto status = client.updateSchema(query);
      std::cout << status << std::endl;
    } catch (std::runtime_error &error) {
      std::cout << error.what() << std::endl;
    }
  }
  return 0;
}
