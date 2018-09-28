#include <iostream>
#include <blazingdb/protocol/api.h>
#include "flatbuffers/flatbuffers.h"


namespace blazingdb {
namespace protocol { 

namespace interpreter {

class DMLRequestMessage : public IMessage {
public: 

  DMLRequestMessage(const std::string& logicalPlan) : IMessage(), logicalPlan (logicalPlan){
  }
  
  DMLRequestMessage (const uint8_t* buffer) : IMessage() {
    auto pointer = flatbuffers::GetRoot<blazingdb::protocol::interpreter::DMLRequest>(buffer);
    logicalPlan = std::string{pointer->logicalPlan()->c_str()};
  }

  std::shared_ptr<flatbuffers::DetachedBuffer> getBufferData() const override {
    flatbuffers::FlatBufferBuilder builder{1024};
    auto string_offset = builder.CreateString(logicalPlan);
    auto root_offset = interpreter::CreateDMLRequest(builder, string_offset);
    builder.Finish(root_offset);
    return std::make_shared<flatbuffers::DetachedBuffer>(builder.Release());
  }

  std::string getLogicalPlan() const {
    return logicalPlan;
  }

private:
  std::string logicalPlan;
};


class DMLResponseMessage : public IMessage {
public:  

  DMLResponseMessage(const std::string& token) : IMessage(), token (token){
  }
  
  DMLResponseMessage (const uint8_t* buffer) : IMessage() {
    auto pointer = flatbuffers::GetRoot<blazingdb::protocol::interpreter::DMLResponse>(buffer);
    token = std::string{pointer->token()->c_str()};
  }

  std::shared_ptr<flatbuffers::DetachedBuffer> getBufferData() const override  {
    flatbuffers::FlatBufferBuilder builder{1024};
    auto string_offset = builder.CreateString(token);
    auto root_offset = interpreter::CreateDMLResponse(builder, string_offset);
    builder.Finish(root_offset);
    return std::make_shared<flatbuffers::DetachedBuffer>(builder.Release());
  }

  std::string getToken () {
    return token;
  }
  
private:
  std::string token;
};

}
}
}
