#pragma once 

#include <iostream>

#include <blazingdb/protocol/api.h>

#include "calcite-client.h"

#include <blazingdb/protocol/orchestrator/messages.h>


namespace blazingdb {
namespace protocol { 

//static calcite::CalciteClient static_calcite_client;

namespace orchestrator { 


}
}
}




// namespace orchestrator { 

// auto OrchestratorService(const blazingdb::protocol::Buffer &requestBuffer) -> blazingdb::protocol::Buffer {
//   RequestMessage request{requestBuffer.data()};
//   DMLRequestMessage requestPayload(request.getPayloadBuffer());

//   std::cout << "header: " << request.messageType() << std::endl;
//   std::cout << "query: " << requestPayload.getLogicalPlan() << std::endl;

//   std::string token = "JIFY*DSA%^F*(*(S)DIKFJLNDVOYD(";

//   DMLResponseMessage responsePayload{token};
//   ResponseMessage responseObject{Status_Success, responsePayload};
//   auto bufferedData = responseObject.getBufferData();
//   Buffer buffer{bufferedData->data(),
//               bufferedData->size()};
//   return buffer;
// }

// }
// } // 
// } // 
 
