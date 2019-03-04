#include <iostream>
#include <map>
#include <tuple>
#include <blazingdb/protocol/api.h>
#include <vector>
#include <string>
#include <future>

#include "blazingdb-communication.hpp"
#include <blazingdb/communication/Context.h>
#include <blazingdb/communication/Buffer.h>

// TODO: remove global
std::string globalOrchestratorPort;
std::string globalCalciteIphost;
std::string globalCalcitePort;
std::string globalRalIphost;
std::string globalRalPort;

#include "ral-client.h"
#include "calcite-client.h"

#include <blazingdb/protocol/message/interpreter/messages.h>
#include <blazingdb/protocol/message/messages.h>
#include <blazingdb/protocol/message/orchestrator/messages.h>
#include <blazingdb/protocol/message/io/file_system.h>

#include <cstdlib>     /* srand, rand */
#include <ctime>       /* time */
#include "config/BlazingConfig.h"

using namespace blazingdb::protocol;
using result_pair = std::pair<Status, std::shared_ptr<flatbuffers::DetachedBuffer>>;

static result_pair registerFileSystem(uint64_t accessToken, Buffer&& buffer)  {
  try {
    interpreter::InterpreterClient ral_client;
    auto response = ral_client.registerFileSystem(accessToken, buffer);

  } catch (std::runtime_error &error) {
    // error with query plan: not resultToken
    std::cout << "In function registerFileSystem: " << error.what() << std::endl;
    std::string stringErrorMessage = "Cannot register the filesystem: " + std::string(error.what());
    ResponseErrorMessage errorMessage{ stringErrorMessage };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  ZeroMessage response{};
  return std::make_pair(Status_Success, response.getBufferData());
}

static result_pair deregisterFileSystem(uint64_t accessToken, Buffer&& buffer)  {
  try {
    interpreter::InterpreterClient ral_client;
    blazingdb::message::io::FileSystemDeregisterRequestMessage message(buffer.data());
    auto response = ral_client.deregisterFileSystem(accessToken, message.getAuthority());

  } catch (std::runtime_error &error) {
    // error with query plan: not resultToken
    std::cout << "In function deregisterFileSystem: " << error.what() << std::endl;
    std::string stringErrorMessage = "Cannot deregister the filesystem: " + std::string(error.what());
    ResponseErrorMessage errorMessage{ stringErrorMessage };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  ZeroMessage response{};
  return std::make_pair(Status_Success, response.getBufferData());
}

static result_pair loadCsvSchema(uint64_t accessToken, Buffer&& buffer) {
  std::shared_ptr<flatbuffers::DetachedBuffer> resultBuffer;
   try {
    interpreter::InterpreterClient ral_client;
    resultBuffer = ral_client.loadCsvSchema(buffer, accessToken);

  } catch (std::runtime_error &error) {
    // error with query plan: not resultToken
    std::cout << "In function loadCsvSchema: " << error.what() << std::endl;
    std::string stringErrorMessage = "Cannot load the csv schema: " + std::string(error.what());
    ResponseErrorMessage errorMessage{ stringErrorMessage };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  return std::make_pair(Status_Success, resultBuffer);
}


static result_pair loadParquetSchema(uint64_t accessToken, Buffer&& buffer) {
  std::shared_ptr<flatbuffers::DetachedBuffer> resultBuffer;
   try {
    interpreter::InterpreterClient ral_client;
    resultBuffer = ral_client.loadParquetSchema(buffer, accessToken);

  } catch (std::runtime_error &error) {
    // error with query plan: not resultToken
    std::cout << "In function loadParquetSchema: " << error.what() << std::endl;
    std::string stringErrorMessage = "Cannot load the parquet schema: " + std::string(error.what());
    ResponseErrorMessage errorMessage{ stringErrorMessage };
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


static result_pair closeConnectionService(uint64_t accessToken, Buffer&& buffer)  {
  try {
    interpreter::InterpreterClient ral_client;
    auto status = ral_client.closeConnection(accessToken);
    std::cout << "status:" << status << std::endl;
  } catch (std::runtime_error &error) {
    std::cout << "In function closeConnectionService: " << error.what() << std::endl;
    std::string stringErrorMessage = "Cannot close the connection: " + std::string(error.what());
    ResponseErrorMessage errorMessage{ stringErrorMessage };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  ZeroMessage response{};
  return std::make_pair(Status_Success, response.getBufferData());
};


using FileSystemTableGroupSchema = blazingdb::message::io::FileSystemTableGroupSchema;
using FileSystemBlazingTableSchema = blazingdb::message::io::FileSystemBlazingTableSchema;


void copyTableGroup(FileSystemTableGroupSchema& schema, FileSystemTableGroupSchema& base_schema) {
    schema.name = base_schema.name;

    for (const auto& table : base_schema.tables) {
        FileSystemBlazingTableSchema new_table;

        new_table.name = table.name;
        new_table.schemaType = table.schemaType;
        new_table.csv = table.csv;
        new_table.parquet = table.parquet;
        new_table.columnNames = table.columnNames;

        schema.tables.emplace_back(new_table);
    }
}


static result_pair  dmlFileSystemService (uint64_t accessToken, Buffer&& buffer) {
  using blazingdb::protocol::orchestrator::DMLResponseMessage;
  using blazingdb::protocol::orchestrator::DMLDistributedResponseMessage;
  using namespace blazingdb::communication;

  blazingdb::message::io::FileSystemDMLRequestMessage requestPayload(buffer.data());
  auto query = requestPayload.statement;
  std::cout << "##DML-FS: " << query << std::endl;
  std::shared_ptr<flatbuffers::DetachedBuffer> resultBuffer;

  auto& manager = Communication::Manager();
  Context* context = manager.generateContext(query, 99);
  std::vector<std::shared_ptr<Node>> cluster = context->getAllNodes();
  std::vector<FileSystemTableGroupSchema> tableSchemas;


  std::vector<blazingdb::message::io::CommunicationNode> fbNodes;
  std::transform(cluster.cbegin(), cluster.cend(),
                  std::back_inserter(fbNodes),
                  [](const std::shared_ptr<Node>& node) -> blazingdb::message::io::CommunicationNode {
                    auto buffer = node->ToBuffer();
                    std::vector<std::int8_t> vecbuffer{buffer->data(), buffer->data() + buffer->size()};
                    return blazingdb::message::io::CommunicationNode(vecbuffer);
                  });
  blazingdb::message::io::CommunicationContext fbContext{fbNodes, 0, context->getContextToken().getIntToken()};


  try {
    calcite::CalciteClient calcite_client;
    auto response = calcite_client.runQuery(query);
    auto logicalPlan = response.getLogicalPlan();
    auto time = response.getTime();
    std::cout << "plan:" << logicalPlan << std::endl;
    std::cout << "time:" << time << std::endl;

    interpreter::InterpreterClient ral_client;
    // Create schemas for each RAL
    for (int k = 0; k < cluster.size(); ++k) {
      FileSystemTableGroupSchema schema;
      copyTableGroup(schema, requestPayload.tableGroup);
      tableSchemas.emplace_back(schema);
    }

    // Divide number of schema files by the RAL quantity
    for (std::size_t k = 0; k < requestPayload.tableGroup.tables.size(); ++k) {
        // RAL for each table group
        int total = requestPayload.tableGroup.tables[k].files.size();
        int quantity = std::max(total / (int)cluster.size(), 1);

        // Assign the files to each schema
        auto itBegin = requestPayload.tableGroup.tables[k].files.begin();
        auto itEnd = requestPayload.tableGroup.tables[k].files.end();
        for (int j = 0; j < cluster.size() && itBegin != itEnd; ++j) {
            std::ptrdiff_t offset = std::min((std::ptrdiff_t)quantity, std::distance(itBegin, itEnd));
            tableSchemas[j].tables[k].files.assign(itBegin, itBegin + offset);
            itBegin += offset;
        }
    }

    std::vector<std::future<result_pair>> futures;
    for (std::size_t index = 0; index < cluster.size(); ++index) {
        futures.emplace_back(std::async([&, index]() {
            try {
                interpreter::InterpreterClient ral_client("/tmp/ral." + std::to_string(cluster[index]->unixSocketId()) + ".socket");

                auto executePlanResponseMessage = ral_client.executeFSDirectPlan(logicalPlan,
                                                                                 tableSchemas[index],
                                                                                 fbContext,
                                                                                 accessToken);
                auto nodeInfo = executePlanResponseMessage.getNodeInfo();
                auto dmlResponseMessage = DMLResponseMessage(executePlanResponseMessage.getResultToken(),
                                                             nodeInfo,
                                                             time);

                return std::make_pair(Status_Success, dmlResponseMessage.getBufferData());
            }
            catch (std::runtime_error &error) {
                // error with query plan: not resultToken
                std::cout << "In function dmlFileSystemService: " << error.what() << std::endl;
                std::string stringErrorMessage = "Error on the communication between Orchestrator and RAL: " + std::string(error.what());
                ResponseErrorMessage errorMessage{ stringErrorMessage };
                return std::make_pair(Status_Error, errorMessage.getBufferData());
            }
        }));
    }

    bool isGood = true;
    result_pair error_message{};
    DMLDistributedResponseMessage distributed_response{};

    for (auto& future : futures) {
        future.wait();

        auto response = future.get();
        if (response.first != Status::Status_Success) {
            isGood = false;
            error_message = response;
        }
        else{
            distributed_response.responses.emplace_back(DMLResponseMessage(response.second->data()));
        }
    }

    return (isGood
            ? std::make_pair(Status_Success, distributed_response.getBufferData())
            : error_message);
  } catch (std::runtime_error &error) {
    // error with query: not logical plan error
    std::cout << "In function dmlFileSystemService: " << error.what() << std::endl;
    std::string stringErrorMessage = "Error on the communication between Orchestrator and Calcite: " + std::string(error.what());
    ResponseErrorMessage errorMessage{ stringErrorMessage };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
}

static result_pair dmlService(uint64_t accessToken, Buffer&& buffer)  {

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
      std::cout << "In function dmlService: " << error.what() << std::endl;
      std::string stringErrorMessage = "Error on the communication between Orchestrator and RAL: " + std::string(error.what());
      ResponseErrorMessage errorMessage{ stringErrorMessage };
      return std::make_pair(Status_Error, errorMessage.getBufferData());
    }
  } catch (std::runtime_error &error) {
    // error with query: not logical plan error
    std::cout << "In function dmlService: " << error.what() << std::endl;
    std::string stringErrorMessage = "Error on the communication between Orchestrator and Calcite: " + std::string(error.what());
    ResponseErrorMessage errorMessage{ stringErrorMessage };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  return std::make_pair(Status_Success, resultBuffer);
};


static result_pair ddlCreateTableService(uint64_t accessToken, Buffer&& buffer)  {
  std::cout << "###DDL Create Table: " << std::endl;
   try {
    calcite::CalciteClient calcite_client;

    orchestrator::DDLCreateTableRequestMessage payload(buffer.data());
    std::cout << "bdname:" << payload.dbName << std::endl;
    std::cout << "table:" << payload.name << std::endl;
    for (auto col : payload.columnNames)
      std::cout << "\ntable.column:" << col << std::endl;

    auto status = calcite_client.createTable(  payload );
  } catch (std::runtime_error &error) {
     // error with ddl query
    std::cout << "In function ddlCreateTableService: " << error.what() << std::endl;
    std::string stringErrorMessage = "In function ddlCreateTableService: cannot create the table: " + std::string(error.what());
    ResponseErrorMessage errorMessage{ stringErrorMessage };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  ZeroMessage response{};
  return std::make_pair(Status_Success, response.getBufferData());
};


static result_pair ddlDropTableService(uint64_t accessToken, Buffer&& buffer)  {
  std::cout << "##DDL Drop Table: " << std::endl;

  try {
    calcite::CalciteClient calcite_client;

    orchestrator::DDLDropTableRequestMessage payload(buffer.data());
    std::cout << "cbname:" << payload.dbName << std::endl;
    std::cout << "table.name:" << payload.name << std::endl;

    auto status = calcite_client.dropTable(  payload );
  } catch (std::runtime_error &error) {
    // error with ddl query
    std::cout << "In function ddlDropTableService: " << error.what() << std::endl;
    std::string stringErrorMessage = "Orchestrator can't communicate with Calcite: " + std::string(error.what());
    ResponseErrorMessage errorMessage{ stringErrorMessage };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  ZeroMessage response{};
  return std::make_pair(Status_Success, response.getBufferData());
};


using FunctionType = result_pair (*)(uint64_t, Buffer&&);

static std::map<int8_t, FunctionType> services;
auto orchestratorService(const blazingdb::protocol::Buffer &requestBuffer) -> blazingdb::protocol::Buffer {
  RequestMessage request{requestBuffer.data()};
  std::cout << "header: " << (int)request.messageType() << std::endl;

  auto result = services[request.messageType()] ( request.accessToken(),  request.getPayloadBuffer() );
  ResponseMessage responseObject{result.first, result.second};
  return Buffer{responseObject.getBufferData()};
};

int
main(int argc, const char *argv[]) {
//  if (6 != argc) {
//    std::cout << "usage: " << argv[0]
//              << " <ORCHESTRATOR_PORT> <CALCITE_[IP|HOSTNAME]> <CALCITE_PORT> "
//                 "<RAL_[IP|HOSTNAME]> <RAL_PORT>"
//              << std::endl;
//    return 1;
//  }
//    globalOrchestratorPort = argv[1];
//    globalCalciteIphost    = argv[2];
//    globalCalcitePort      = argv[3];
//    globalRalIphost        = argv[4];
//    globalRalPort          = argv[5];

  Communication::InitializeManager();

  std::cout << "Orchestrator is listening" << std::endl;

  blazingdb::protocol::UnixSocketConnection connection("/tmp/orchestrator.socket");
  blazingdb::protocol::Server server(connection);

  services.insert(std::make_pair(orchestrator::MessageType_DML, &dmlService));
  services.insert(std::make_pair(orchestrator::MessageType_DML_FS, &dmlFileSystemService));

  services.insert(std::make_pair(orchestrator::MessageType_DDL_CREATE_TABLE, &ddlCreateTableService));
  services.insert(std::make_pair(orchestrator::MessageType_DDL_DROP_TABLE, &ddlDropTableService));

  services.insert(std::make_pair(orchestrator::MessageType_AuthOpen, &openConnectionService));
  services.insert(std::make_pair(orchestrator::MessageType_AuthClose, &closeConnectionService));

  services.insert(std::make_pair(orchestrator::MessageType_RegisterFileSystem, &registerFileSystem));
  services.insert(std::make_pair(orchestrator::MessageType_DeregisterFileSystem, &deregisterFileSystem));

  services.insert(std::make_pair(orchestrator::MessageType_LoadCsvSchema, &loadCsvSchema));
  services.insert(std::make_pair(orchestrator::MessageType_LoadParquetSchema, &loadParquetSchema));

  server.handle(&orchestratorService);

  Communication::FinalizeManager();

  return 0;
}
