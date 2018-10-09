#include <iostream>

#include <blazingdb/protocol/api.h>

#include "calcite-client.h"
#include "ral-client.h"

#include "orchestrator-server.h"

 

using namespace blazingdb::protocol;

int main() {
  blazingdb::protocol::UnixSocketConnection connection("/tmp/ral.socket");
  interpreter::InterpreterClient client{connection};

  {
    std::string logicalPlan = "LogicalUnion(all=[false])";
    try {
      auto token = client.executePlan(logicalPlan, 123456L);
      std::cout << token << std::endl;
    } catch (std::runtime_error &error) {
      std::cout << error.what() << std::endl;
    }

    // logicalPlan = "example_error_logical_plan\n";
    // try {
    //   std::cout << "1\n";
    //   std::string token = client.executePlan(logicalPlan);
    //   std::cout << "2\n";
    //   std::cout << token << std::endl;
    // } catch (std::runtime_error &error) {
    //   std::cout << error.what() << std::endl;
    // }
  } 
  return 0;
}
