#include <thread>

#include <blazingdb/communication/Manager.h>

namespace Communication {

const std::unique_ptr<blazingdb::communication::Manager> &Manager() {
  static std::unique_ptr<blazingdb::communication::Manager> manager =
      blazingdb::communication::Manager::Make();
  return manager;
}

std::thread managerThread;

void InitializeManager() {
  const std::unique_ptr<blazingdb::communication::Manager> &manager = Manager();
  managerThread = std::thread{[&manager]() { manager->Run(); }};
}

void FinalizeManager() {
  Manager()->Close();
  managerThread.join();
}

}  // namespace Communication
