#include <iostream>
#include <map>
#include <tuple>
#include <blazingdb/protocol/api.h>
#include <vector>
#include <string>
#include <future>
#include <mutex>

#include "blazingdb-communication.hpp"
#include <blazingdb/communication/Context.h>
#include <blazingdb/communication/Buffer.h>
#include <blazingdb/communication/Address-Internal.h>

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

ConnectionAddress calciteConnectionAddress;
ConnectionAddress ralConnectionAddress;
int orchestratorCommunicationTcpPort;

#ifdef USE_UNIX_SOCKETS

static void setupUnixSocketConnections(
        int orchestrator_communication_tcp_port = 9000,
        const std::string orchestrator_unix_socket_path = "/tmp/orchestrator.socket",
        const std::string calcite_unix_socket_path = "/tmp/calcite.socket",
        const std::string ral_unix_socket_path = "/tmp/ral.1.socket") {

  orchestratorConnectionAddress.unix_socket_path = orchestrator_unix_socket_path;
  calciteConnectionAddress.unix_socket_path = calcite_unix_socket_path;
  ralConnectionAddress.unix_socket_path = ral_unix_socket_path;
  
  orchestratorCommunicationTcpPort = orchestrator_communication_tcp_port;
}

#else

static void setupTCPConnections(
    int orchestrator_communication_tcp_port,
    int orchestrator_protocol_tcp_port,
    const std::string &calcite_tcp_host,
    int calcite_tcp_port) {

  //TODO percy refactor this until table scan is ready in distribution
  const std::string ral_tcp_host = "127.0.0.1";
  const int ral_tcp_port = 8891;

  calciteConnectionAddress.tcp_host = calcite_tcp_host;
  calciteConnectionAddress.tcp_port = calcite_tcp_port;

  ralConnectionAddress.tcp_host = ral_tcp_host;
  ralConnectionAddress.tcp_port = ral_tcp_port;

  orchestratorCommunicationTcpPort = orchestrator_communication_tcp_port;
}

#endif

#ifdef USE_UNIX_SOCKETS

static ConnectionAddress getRalConnectionAddress(std::shared_ptr<blazingdb::communication::Node> node) {
    const std::string conn = "/tmp/ral." + std::to_string(node->unixSocketId()) + ".socket";
    ConnectionAddress connectionAddress;
    connectionAddress.unix_socket_path = unix_socket_path;
    return connectionAddress;
}
                
#else

static ConnectionAddress getRalConnectionAddress(std::shared_ptr<blazingdb::communication::Node> node) {
    const blazingdb::communication::internal::ConcreteAddress *concreteAddress = static_cast<const blazingdb::communication::internal::ConcreteAddress *>(node->address());
    const std::string host = concreteAddress->ip();
    const int port = concreteAddress->protocol_port();
    ConnectionAddress connectionAddress;
    connectionAddress.tcp_host = host;
    connectionAddress.tcp_port = port;
    return connectionAddress;
}

#endif

static result_pair registerFileSystem(uint64_t accessToken, Buffer&& buffer)  {
  using namespace blazingdb::communication;
  
  try {
    auto& manager = Communication::Manager(orchestratorCommunicationTcpPort);
    Context* context = manager.generateContext(std::to_string(accessToken), 99);
    std::vector<std::shared_ptr<Node>> cluster = context->getAllNodes();
    
    std::vector<std::future<result_pair>> futures;
    for (std::size_t index = 0; index < cluster.size(); ++index) {
        futures.emplace_back(std::async(std::launch::async, [&, index]() {
            try {
                interpreter::InterpreterClient ral_client(getRalConnectionAddress(cluster[index]));
                auto response = ral_client.registerFileSystem(accessToken, buffer);
                
                if (response == Status_Error) {                
                    std::cout << "In function registerFileSystem for RAL: " << std::to_string(index) << std::endl;
                    std::string stringErrorMessage = "Cannot register the filesystem for RAL: " + std::to_string(index);
                    ResponseErrorMessage errorMessage{ stringErrorMessage };
                    return std::make_pair(Status_Error, errorMessage.getBufferData());
                }
                
                ZeroMessage ok_response{};
                return std::make_pair(Status_Success, ok_response.getBufferData());
            }
            catch (std::runtime_error &error) {
                // error with query plan: not resultToken
                std::cout << "In function registerFileSystem: " << error.what() << std::endl;
                std::string stringErrorMessage = "Cannot register the filesystem: " + std::string(error.what());
                ResponseErrorMessage errorMessage{ stringErrorMessage };
                return std::make_pair(Status_Error, errorMessage.getBufferData());
            }
        }));
    }

    bool isGood = true;
    result_pair error_message{};

    for (auto& future : futures) {
        auto response = future.get();
        if (response.first != Status::Status_Success) {
            isGood = false;
            error_message = response;
        }
    }
    
    if (isGood == false) {
        return error_message;
    }
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
  using namespace blazingdb::communication;

  try {
    auto& manager = Communication::Manager(orchestratorCommunicationTcpPort);
    Context* context = manager.generateContext(std::to_string(accessToken), 99);
    std::vector<std::shared_ptr<Node>> cluster = context->getAllNodes();
    
    std::vector<std::future<result_pair>> futures;
    for (std::size_t index = 0; index < cluster.size(); ++index) {
        futures.emplace_back(std::async(std::launch::async, [&, index]() {
            try {
                interpreter::InterpreterClient ral_client(getRalConnectionAddress(cluster[index]));
                blazingdb::message::io::FileSystemDeregisterRequestMessage message(buffer.data());
                auto response = ral_client.deregisterFileSystem(accessToken, message.getAuthority());
                
                if (response == Status_Error) {                
                    std::cout << "In function deregisterFileSystem for RAL: " << std::to_string(index) << std::endl;
                    std::string stringErrorMessage = "Cannot deregister the filesystem for RAL: " + std::to_string(index);
                    ResponseErrorMessage errorMessage{ stringErrorMessage };
                    return std::make_pair(Status_Error, errorMessage.getBufferData());
                }
                
                ZeroMessage ok_response{};
                return std::make_pair(Status_Success, ok_response.getBufferData());
            }
            catch (std::runtime_error &error) {
                // error with query plan: not resultToken
                std::cout << "In function deregisterFileSystem: " << error.what() << std::endl;
                std::string stringErrorMessage = "Cannot deregister the filesystem: " + std::string(error.what());
                ResponseErrorMessage errorMessage{ stringErrorMessage };
                return std::make_pair(Status_Error, errorMessage.getBufferData());
            }
        }));
    }

    bool isGood = true;
    result_pair error_message{};

    for (auto& future : futures) {
        auto response = future.get();
        if (response.first != Status::Status_Success) {
            isGood = false;
            error_message = response;
        }
    }
    
    if (isGood == false) {
        return error_message;
    }
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




static result_pair  openConnectionService(uint64_t nonAccessToken, Buffer&& buffer)  {
  srand(time(0));
  int64_t token = rand();
  orchestrator::AuthResponseMessage response{token};
  std::cout << "authorizationService: " << token << std::endl;
  return std::make_pair(Status_Success, response.getBufferData());
};


static result_pair closeConnectionService(uint64_t accessToken, Buffer&& buffer)  {
  using namespace blazingdb::communication;

  try {
    auto& manager = Communication::Manager(orchestratorCommunicationTcpPort);
    Context* context = manager.generateContext(std::to_string(accessToken), 99);
    std::vector<std::shared_ptr<Node>> cluster = context->getAllNodes();
    
    std::vector<std::future<result_pair>> futures;
    for (std::size_t index = 0; index < cluster.size(); ++index) {
        futures.emplace_back(std::async(std::launch::async, [&, index]() {
            try {
                interpreter::InterpreterClient ral_client(getRalConnectionAddress(cluster[index]));
                auto status = ral_client.closeConnection(accessToken);
                std::cout << "status close conneciton for RAL: " << std::to_string(index) << ": " << status << std::endl;
                
                if (status == Status_Error) {                
                    std::cout << "In function closeConnectionService for RAL: " << std::to_string(index) << std::endl;
                    std::string stringErrorMessage = "Cannot close the connection for RAL: " + std::to_string(index);
                    ResponseErrorMessage errorMessage{ stringErrorMessage };
                    return std::make_pair(Status_Error, errorMessage.getBufferData());
                }
                
                ZeroMessage ok_response{};
                return std::make_pair(Status_Success, ok_response.getBufferData());
            }
            catch (std::runtime_error &error) {
                std::cout << "In function closeConnectionService: " << error.what() << std::endl;
                std::string stringErrorMessage = "Cannot close the connection: " + std::string(error.what());
                ResponseErrorMessage errorMessage{ stringErrorMessage };
                return std::make_pair(Status_Error, errorMessage.getBufferData());
            }
        }));
    }

    bool isGood = true;
    result_pair error_message{};

    for (auto& future : futures) {
        auto response = future.get();
        if (response.first != Status::Status_Success) {
            isGood = false;
            error_message = response;
        }
    }
    
    if (isGood == false) {
        return error_message;
    }
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


//TODO: if we watned unique tables for sessions you would store
//these in a map with access token as the key
//to make this work you have to do the same in calcite
std::mutex tables_mutex;
blazingdb::message::io::FileSystemTableGroupSchema tables;

void add_table(orchestrator::DDLCreateTableRequestMessage request,
		blazingdb::protocol::TableSchemaSTL schema,
		bool & existed_previously){
	std::lock_guard<std::mutex> lock(tables_mutex);
	existed_previously = false;
	//if table exists overwrite it
	for(int table_index = 0; table_index < tables.tables.size(); table_index++){
		if(tables.tables[table_index].name == request.name){
			existed_previously = true;
			tables.tables[table_index].tableSchema = schema;
			tables.tables[table_index].schemaType = request.schemaType;
			tables.tables[table_index].gdf = request.gdf;
      break;
		}
	}

	if(!existed_previously){
		blazingdb::message::io::FileSystemBlazingTableSchema new_schema;
    new_schema.name = request.name;
		new_schema.tableSchema = schema;
		new_schema.schemaType = request.schemaType;
		new_schema.gdf = request.gdf;
		tables.tables.push_back(new_schema);
	}
}

void remove_table(std::string name){
	std::lock_guard<std::mutex> lock(tables_mutex);
	for(int table_index = 0; table_index < tables.tables.size(); table_index++){
			if(tables.tables[table_index].name == name){
				tables.tables.erase(tables.tables.begin() + table_index);
				break;
			}
	}
}


static result_pair dmlFileSystemService (uint64_t accessToken, Buffer&& buffer) {
  using blazingdb::protocol::orchestrator::DMLResponseMessage;
  using blazingdb::protocol::orchestrator::DMLDistributedResponseMessage;
  using namespace blazingdb::communication;
  
  blazingdb::message::io::FileSystemDMLRequestMessage requestPayload(buffer.data());
  auto query = requestPayload.statement();
  std::cout << "##DML-FS: " << query << std::endl;
  std::shared_ptr<flatbuffers::DetachedBuffer> resultBuffer;

  auto& manager = Communication::Manager(orchestratorCommunicationTcpPort);
  Context* context = manager.generateContext(query, 99);
  std::vector<std::shared_ptr<Node>> cluster = context->getAllNodes();
  std::vector<FileSystemTableGroupSchema> tableSchemas;


  std::vector<blazingdb::message::io::CommunicationNodeSchema> fbNodes;
  std::transform(cluster.cbegin(), cluster.cend(),
                  std::back_inserter(fbNodes),
                  [](const std::shared_ptr<Node>& node) -> blazingdb::message::io::CommunicationNodeSchema {
                    auto buffer = node->ToBuffer();
                    std::vector<std::int8_t> vecbuffer{buffer->data(), buffer->data() + buffer->size()};
                    return blazingdb::message::io::CommunicationNodeSchema{vecbuffer};
                  });
  blazingdb::message::io::CommunicationContextSchema fbContext{fbNodes, 0, context->getContextToken().getIntToken()};


  try {
    calcite::CalciteClient calcite_client(calciteConnectionAddress);
    auto response = calcite_client.runQuery(query);
    auto logicalPlan = response.getLogicalPlan();
    auto time = response.getTime();
    std::cout << "plan:" << logicalPlan << std::endl;
    std::cout << "time:" << time << std::endl;

    // Create schemas for each RAL
    for (int k = 0; k < cluster.size(); ++k) {
      tableSchemas.emplace_back(tables);
    }
  
    // Divide number of schema files by the RAL quantity
    for (std::size_t k = 0; k < tables.tables.size(); ++k) {
        if (tables.tables[k].tableSchema.files.size() > 0) { // if table has files, lets split them up
          // RAL for each table group
          int total = tables.tables[k].tableSchema.files.size();
          int quantity = std::max(total / (int)cluster.size(), 1);

          // Assign the files to each schema
          auto itBegin = tables.tables[k].tableSchema.files.begin();
          auto itEnd = tables.tables[k].tableSchema.files.end();
          for (int j = 0; j < cluster.size() && itBegin != itEnd; ++j) {
              std::ptrdiff_t offset = std::min((std::ptrdiff_t)quantity, std::distance(itBegin, itEnd));
              tableSchemas[j].tables[k].tableSchema.files.assign(itBegin, itBegin + offset);
              itBegin += offset;
          }
        }
    }

    std::vector<std::future<result_pair>> futures;
    for (std::size_t index = 0; index < cluster.size(); ++index) {
        futures.emplace_back(std::async(std::launch::async, [&, index]() {
            try {
                interpreter::InterpreterClient ral_client(getRalConnectionAddress(cluster[index]));

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

std::string convert_dtype_string(int dtype){
	switch(dtype){
	case 0:
			return "invalid";
	case 1:
			return "GDF_INT8";
	case 2:
			return "GDF_INT16";
	case 3:
			return "GDF_INT32";
	case 4:
			return "GDF_INT64";
	case 5:
			return "GDF_FLOAT32";
	case 6:
			return "GDF_FLOAT64";
  case 7:
      return "GDF_BOOL8";
	case 8:
			return "GDF_DATE32";
	case 9:
			return "GDF_DATE64";
	case 10:
			return "GDF_TIMESTAMP";
	case 11:
			return "GDF_CATEGORY";
	case 12:
			return "GDF_STRING";
	case 13:
			return "GDF_STRING_CATEGORY";
	default:
			return "invalid";

	}
}

int convert_string_dtype(std::string str){
	if(str == "GDF_INT8"){
		return 1;
	}else if(str == "GDF_INT16"){
		return 2;
	}else if(str == "GDF_INT32"){
		return 3;
	}else if(str == "GDF_INT64"){
		return 4;
	}else if(str == "GDF_FLOAT32"){
		return 5;
	}else if(str == "GDF_FLOAT64"){
		return 6;
	}else if(str == "GDF_BOOL8"){
		return 7;
  }else if(str == "GDF_DATE32"){
		return 8;
	}else if(str == "GDF_DATE64"){
		return 9;
	}else if(str == "GDF_TIMESTAMP"){
		return 10;
	}else if(str == "GDF_CATEGORY"){
		return 11;
	}else if(str == "GDF_STRING"){
		return 12;
	}else if(str == "GDF_STRING_CATEGORY"){
		return 13;
	}else{
		return -1;
	}
}

static result_pair ddlCreateTableService(uint64_t accessToken, Buffer&& buffer)  {
    using namespace blazingdb::communication;
    
    orchestrator::DDLCreateTableRequestMessage payload(buffer.data());
	std::cout << "###DDL Create Table: " << std::endl;
	blazingdb::protocol::TableSchemaSTL temp_schema;

    try{
    	if(payload.schemaType == blazingdb::protocol::FileSchemaType::FileSchemaType_PARQUET ||
    			payload.schemaType == blazingdb::protocol::FileSchemaType::FileSchemaType_CSV){
            
            auto& manager = Communication::Manager(orchestratorCommunicationTcpPort);
            Context* context = manager.generateContext(std::to_string(accessToken), 99);
            std::vector<std::shared_ptr<Node>> cluster = context->getAllNodes();
            
            interpreter::InterpreterClient ral_client(getRalConnectionAddress(cluster[0]));
    		auto ral_response = ral_client.parseSchema(buffer,accessToken);
    		payload.columnNames = ral_response.getTableSchema().names;
    		/*typedef enum {
    GDF_invalid=0,
    GDF_INT8,
    GDF_INT16,
    GDF_INT32,
    GDF_INT64,
    GDF_FLOAT32,
    GDF_FLOAT64,
    GDF_DATE32,     ///< int32_t days since the UNIX epoch
    GDF_DATE64,     ///< int64_t milliseconds since the UNIX epoch
    GDF_TIMESTAMP,  ///< Exact timestamp encoded with int64 since UNIX epoch (Default unit millisecond)
    GDF_CATEGORY,
    GDF_STRING,
    GDF_STRING_CATEGORY, ///< Stores indices of an NVCategory in data and in extra col info a reference to the nv_category
    N_GDF_TYPES,   ///< additional types should go BEFORE N_GDF_TYPES
} gdf_dtype;*/


    		for(int i = 0; i < ral_response.getTableSchema().types.size(); i++){
    			payload.columnTypes.push_back(convert_dtype_string(ral_response.getTableSchema().types[i]));
    		}
//    		payload.columnTypes = ral_response.getTableSchema().types;

    		temp_schema = ral_response.getTableSchema();

    	}else{
    		//TODO: i think that column names and types are set when they call this kind
    		//so it should be ok

    		temp_schema.names = payload.columnNames;
    		for(int i = 0; i <  payload.columnTypes.size(); i++){
    			temp_schema.types.push_back(convert_string_dtype(payload.columnTypes[i]));
    		}

    	}


    }catch (std::runtime_error &error) {
        // error with ddl query
       std::cout << "In function ddlCreateTableService: " << error.what() << std::endl;
       std::string stringErrorMessage = "In function ddlCreateTableService: cannot create the table: " + std::string(error.what());
       ResponseErrorMessage errorMessage{ stringErrorMessage };
       return std::make_pair(Status_Error, errorMessage.getBufferData());
     }

    bool existed;
    add_table(payload,temp_schema,existed);

   try {
    calcite::CalciteClient calcite_client(calciteConnectionAddress);

    if(existed){

          orchestrator::DDLDropTableRequestMessage drop_request;
          drop_request.name = payload.name;
          drop_request.dbName = "main";

          auto status = calcite_client.dropTable(  drop_request);

      }

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
  //std::cout << "##DEBUG calcite connection address: " << calciteConnectionAddress << std::endl;

  try {
    calcite::CalciteClient calcite_client(calciteConnectionAddress);

    orchestrator::DDLDropTableRequestMessage payload(buffer.data());
    std::cout << "cbname:" << payload.dbName << std::endl;
    std::cout << "table.name:" << payload.name << std::endl;
    remove_table(payload.name);
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
 
  auto result = services[request.messageType()] (request.accessToken(),  request.getPayloadBuffer() );
  ResponseMessage responseObject{result.first, result.second};
  return Buffer{responseObject.getBufferData()};
};

int
main(int argc, const char *argv[]) {

#ifdef USE_UNIX_SOCKETS

  std::cout << "usage: " << argv[0] << " <ORCHESTRATOR_COMMUNICATION_TCP_PORT>" << std::endl;

  if (argc == 2) {
    const int orchestratorCommunicationPort = ConnectionUtils::parsePort(argv[1]);
    
    if (orchestratorCommunicationPort == -1) {
      std::cout << "FATAL: Invalid Orchestrator TCP ports " + std::string(argv[1]) + " " + std::string(argv[1]) << std::endl;
      return EXIT_FAILURE;
    }
    
    setupUnixSocketConnections(orchestratorCommunicationPort);
  } else {
    // when the user doesnt enter any args
    setupUnixSocketConnections();
  }

  blazingdb::protocol::UnixSocketConnection orchestratorConnection(orchestratorConnectionAddress);

  std::cout << "orchestrator unix socket path: " << orchestratorConnectionAddress.unix_socket_path << std::endl;
  std::cout << "calcite unix socket path: " << calciteConnectionAddress.unix_socket_path << std::endl;
  std::cout << "ral unix socket path: " << ralConnectionAddress.unix_socket_path << std::endl;
  
#else

  std::cout << "usage: " << argv[0] << " <ORCHESTRATOR_HTTP_COMMUNICATION_PORT> <ORCHESTRATOR_TCP_PROTOCOL_PORT> <CALCITE_TCP_PROTOCOL_[IP|HOSTNAME]> <CALCITE_TCP_PROTOCOL_PORT>" << std::endl;

  if (argc != 5) {
      std::cout << "FATAL: Invalid number of arguments" << std::endl;
      return EXIT_FAILURE;
  }

  const int orchestratorCommunicationPort = ConnectionUtils::parsePort(argv[1]);  
  const int orchestratorProtocolPort = ConnectionUtils::parsePort(argv[2]);
  
  if (orchestratorProtocolPort == -1 || orchestratorCommunicationPort == -1) {
      std::cout << "FATAL: Invalid Orchestrator TCP ports " + std::string(argv[1]) + " " + std::string(argv[2]) << std::endl;
      return EXIT_FAILURE;
  }
  
  const std::string calciteHost = argv[3];
  const int calcitePort = ConnectionUtils::parsePort(argv[4]);
  
  if (calcitePort == -1) {
      std::cout << "FATAL: Invalid Calcite TCP port " + std::string(argv[4]) << std::endl;
      return EXIT_FAILURE;
  }
  
  setupTCPConnections(orchestratorCommunicationPort, orchestratorProtocolPort, calciteHost, calcitePort);

  std::cout << "Orchestrator HTTP communication port: " << orchestratorCommunicationTcpPort << std::endl;
  std::cout << "Orchestrator TCP protocol port: " << orchestratorProtocolPort << std::endl;
  std::cout << "Calcite TCP protocol host: " << calciteConnectionAddress.tcp_host << std::endl;
  std::cout << "Calcite TCP protocol port: " << calciteConnectionAddress.tcp_port << std::endl;

#endif

  Communication::InitializeManager(orchestratorCommunicationTcpPort);
  std::cout << "Communication manager is listening on port: " << orchestratorCommunicationTcpPort << std::endl;

  std::cout << "Orchestrator is listening" << std::endl;

  blazingdb::protocol::Server server(orchestratorProtocolPort);

  services.insert(std::make_pair(orchestrator::MessageType_DML_FS, &dmlFileSystemService));

  services.insert(std::make_pair(orchestrator::MessageType_DDL_CREATE_TABLE, &ddlCreateTableService));
  services.insert(std::make_pair(orchestrator::MessageType_DDL_DROP_TABLE, &ddlDropTableService));

  services.insert(std::make_pair(orchestrator::MessageType_AuthOpen, &openConnectionService));
  services.insert(std::make_pair(orchestrator::MessageType_AuthClose, &closeConnectionService));

  services.insert(std::make_pair(orchestrator::MessageType_RegisterFileSystem, &registerFileSystem));
  services.insert(std::make_pair(orchestrator::MessageType_DeregisterFileSystem, &deregisterFileSystem));

  server.handle(&orchestratorService);

  Communication::FinalizeManager(orchestratorCommunicationTcpPort);

  return 0;
}
