#include <iostream>

#include <blazingdb/protocol/api.h>

#include "ral-client.h"


using namespace blazingdb::protocol;

int main() {
  blazingdb::protocol::UnixSocketConnection connection("/tmp/ral.socket");
  interpreter::InterpreterClient client{connection};

  std::string logicalPlan = "LogicalUnion(all=[false])";
  try {
    auto tableGroup = ::blazingdb::protocol::TableGroupDTO{
      .tables = {
          BlazingTableDTO {
              .name="user",
              .columns = {
                  ::libgdf::gdf_column {
                      .data=nullptr,
                      .valid=nullptr,
                      .size = 10,
                      .dtype = (libgdf::gdf_dtype)0,
                      .null_count = 0,
                      .dtype_info = libgdf::gdf_dtype_extra_info {
                          .time_unit = (libgdf::gdf_time_unit)0,
                      },
                  },
              },
              .columnNames = {"id", "name"}
          }
      },
      .name = "alexdb",
    };
    auto token = client.executePlan(logicalPlan, tableGroup, 123456L);
    std::cout << token << std::endl;
  } catch (std::runtime_error &error) {
    std::cout << error.what() << std::endl;
  }

  return 0;
}

