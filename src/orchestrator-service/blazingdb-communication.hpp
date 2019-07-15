#pragma once

#include <thread>

#include <blazingdb/communication/Manager.h>

// TODO percy review this code doesnt seems good

namespace Communication {

blazingdb::communication::Manager& Manager(int communicationTcpPort) {
  static std::unique_ptr<blazingdb::communication::Manager> manager =
      blazingdb::communication::Manager::Make(communicationTcpPort);
  return *manager;
}

std::thread managerThread;

void InitializeManager(int communicationTcpPort) {
  blazingdb::communication::Manager& manager = Manager(communicationTcpPort);
  managerThread = std::thread{[&manager]() { manager.Run(); }};
}

void FinalizeManager(int communicationTcpPort) {
  Manager(communicationTcpPort).Close();
  managerThread.join();
}

}  // namespace Communication
