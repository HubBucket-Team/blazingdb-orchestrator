#include <iostream>
#include <map>
#include <tuple>
#include <blazingdb/protocol/api.h>
#include <mutex>
#include <regex>

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

std::vector<std::string> split(std::string input, std::string regex) {
  // passing -1 as the submatch index parameter performs splitting
  size_t pos = 0;
  std::vector<std::string> result;
  while ((pos = input.find(regex)) != std::string::npos) {
    std::string token;
    token = input.substr(0, pos);
    result.push_back(token);
    //  std::cout << token << std::endl;
    input.erase(0, pos + regex.length());
  }
  result.push_back(input);
  return result;
  //std::cout << s << std::endl;

}

//DORU_TESTING
std::vector<std::string>  split(std::string &s, char delim) {
  std::vector<std::string> elems;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, delim)) {
    if (!(item.size() == 1 && item[0] == delim)){
      elems.push_back(item);
    }
  }
  if(s[s.size()-1] == delim){
    elems.push_back("");
  }
  return elems;
}


std::vector<std::string> get_table_names_from_plan(const std::string& plan) {
  std::vector<std::string> response;
  auto pos = plan.find("LogicalTableScan");
  if (pos != std::string::npos) {
    auto str_tmp = plan.substr(pos);
    auto lines = split(str_tmp, '\n');
    std::smatch match;
    std::regex re {R""(table=\[(\W\(.+?\)|.+)\])""};
    for (auto& str : lines) {
      if( std::regex_search(str, match, re) ) {
        auto table_str = match[1].str();
        pos = table_str.find(", ");
        auto end_pos = table_str.find("]");
        if (pos != std::string::npos && end_pos != std::string::npos) {
          auto length = end_pos - pos - 2;
          //std::cout << length << " found:"  <<  table_str.substr(pos+2, length)  << '\n';
          response.push_back(table_str.substr(pos+2, length));
        }
      }
    }
  }
  return response;
}


static result_pair dmlFileSystemService (uint64_t accessToken, Buffer&& buffer) {
  blazingdb::message::io::FileSystemDMLRequestMessage requestPayload(buffer.data());
  auto query = requestPayload.statement;
  std::cout << "##DML-FS: " << query << std::endl;
  std::shared_ptr<flatbuffers::DetachedBuffer> resultBuffer;

  try {
    calcite::CalciteClient calcite_client;
    auto response = calcite_client.runQuery(query);
    auto logicalPlan = response.getLogicalPlan();

    auto table_names = get_table_names_from_plan(logicalPlan);
    bool is_not_contained = false;
    blazingdb::message::io::FileSystemTableGroupSchema query_tables;

    for(auto & table : tables.tables) {
      auto iter = std::find(table_names.begin(), table_names.end(), table.name);
      if (iter == table_names.end()) {
        is_not_contained = true;
      } else { // found
        query_tables.tables.push_back(table);
      }
    }
//    if (is_not_contained) {
//      std::string stringErrorMessage = "Error on the query: " + query + ". The table names used in this query is not registered.";
//      ResponseErrorMessage errorMessage{ stringErrorMessage };
//      return std::make_pair(Status_Error, errorMessage.getBufferData());
//    }
    auto time = response.getTime();
    std::cout << "plan:" << logicalPlan << std::endl;
    std::cout << "time:" << time << std::endl;
    try {
      interpreter::InterpreterClient ral_client;
      requestPayload.tableGroup = tables;

      auto executePlanResponseMessage = ral_client.executeFSDirectPlan(logicalPlan, requestPayload.tableGroup, accessToken);

      auto nodeInfo = executePlanResponseMessage.getNodeInfo();
      auto dmlResponseMessage = orchestrator::DMLResponseMessage(
          executePlanResponseMessage.getResultToken(),
          nodeInfo, time);
      resultBuffer = dmlResponseMessage.getBufferData();
    } catch (std::runtime_error &error) {
      // error with query plan: not resultToken
      std::cout << "In function dmlFileSystemService: " << error.what() << std::endl;
      std::string stringErrorMessage = "Error on the communication between Orchestrator and RAL: " + std::string(error.what());
      ResponseErrorMessage errorMessage{ stringErrorMessage };
      return std::make_pair(Status_Error, errorMessage.getBufferData());
    }
  } catch (std::runtime_error &error) {
    // error with query: not logical plan error
    std::cout << "In function dmlFileSystemService: " << error.what() << std::endl;
    std::string stringErrorMessage = "Error on the communication between Orchestrator and Calcite: " + std::string(error.what());
    ResponseErrorMessage errorMessage{ stringErrorMessage };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  return std::make_pair(Status_Success, resultBuffer);
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

    orchestrator::DDLCreateTableRequestMessage payload(buffer.data());
	std::cout << "###DDL Create Table: " << std::endl;
	blazingdb::protocol::TableSchemaSTL temp_schema;

    try{
    	if(payload.schemaType == blazingdb::protocol::FileSchemaType::FileSchemaType_PARQUET ||
    			payload.schemaType == blazingdb::protocol::FileSchemaType::FileSchemaType_CSV){
    	    interpreter::InterpreterClient ral_client;
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
    calcite::CalciteClient calcite_client;

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

  try {
    calcite::CalciteClient calcite_client;

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

  std::cout << "Orchestrator is listening" << std::endl;

  blazingdb::protocol::UnixSocketConnection connection("/tmp/orchestrator.socket");
  blazingdb::protocol::Server server(connection);

  services.insert(std::make_pair(orchestrator::MessageType_DML_FS, &dmlFileSystemService));

  services.insert(std::make_pair(orchestrator::MessageType_DDL_CREATE_TABLE, &ddlCreateTableService));
  services.insert(std::make_pair(orchestrator::MessageType_DDL_DROP_TABLE, &ddlDropTableService));

  services.insert(std::make_pair(orchestrator::MessageType_AuthOpen, &openConnectionService));
  services.insert(std::make_pair(orchestrator::MessageType_AuthClose, &closeConnectionService));

  services.insert(std::make_pair(orchestrator::MessageType_RegisterFileSystem, &registerFileSystem));
  services.insert(std::make_pair(orchestrator::MessageType_DeregisterFileSystem, &deregisterFileSystem));

  server.handle(&orchestratorService);
  return 0;
}
