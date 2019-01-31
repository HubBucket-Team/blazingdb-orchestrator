#include "config/BlazingConfig.h"

namespace ral {
namespace config {

    BlazingConfig::BlazingConfig()
    { }

    int BlazingConfig::getRalQuantity() const {
        return ral_quantity;
    }

    BlazingConfig& BlazingConfig::setRalQuantity(int value) {
        ral_quantity = value;
        return *this;
    }

    const std::vector<std::string>& BlazingConfig::getSocketPath() const {
        return socket_path;
    }

    BlazingConfig& BlazingConfig::addSocketPath(std::string&& value) {
        socket_path.emplace_back(std::move(value));
        return *this;
    }

    BlazingConfig& BlazingConfig::addSocketPath(const std::string& value) {
        socket_path.emplace_back(value);
        return *this;
    }

} // namespace config
} // namespace ral
