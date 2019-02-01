#ifndef RAL_CONFIG_BLAZINGCONFIG_H
#define RAL_CONFIG_BLAZINGCONFIG_H

#include <vector>
#include <string>

namespace orch {
namespace config {

    class BlazingConfig {
    public:
        static BlazingConfig& getInstance() {
            static BlazingConfig config;
            return config;
        }

    private:
        BlazingConfig();

        BlazingConfig(BlazingConfig&&) = delete;

        BlazingConfig(const BlazingConfig&) = delete;

        BlazingConfig operator=(BlazingConfig&&) = delete;

        BlazingConfig operator=(const BlazingConfig&) = delete;

    public:
        int getRalQuantity() const;

        BlazingConfig& setRalQuantity(int value);

    public:
        const std::vector<std::string>& getSocketPath() const;

        BlazingConfig& addSocketPath(std::string&& value);

        BlazingConfig& addSocketPath(const std::string& value);

    private:
        int ral_quantity;
        std::vector<std::string> socket_path;
    };

} // namespace config
} // namespace ral

#endif