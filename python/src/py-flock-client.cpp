/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <flock/cxx/client.hpp>
#include <flock/cxx/group-view.hpp>
#include <flock/cxx/group.hpp>

namespace py11 = pybind11;
using namespace pybind11::literals;

struct margo_instance;
typedef struct margo_instance* margo_instance_id;
typedef py11::capsule pymargo_instance_id;

#define MID2CAPSULE(__mid)   py11::capsule((void*)(__mid), "margo_instance_id")
#define CAPSULE2MID(__caps)  (margo_instance_id)(__caps)

PYBIND11_MODULE(pyflock_client, m) {
    m.doc() = "Flock client python extension";
#if 0
    py11::register_exception<Exception>(m, "Exception", PyExc_RuntimeError);
    py11::class_<Client>(m, "Client")
        .def(py11::init<pymargo_instance_id>())
        .def_property_readonly("margo_instance_id",
            [](const Client& client) {
                return MID2CAPSULE(client.engine().get_margo_instance());
            })
        .def("make_service_handle",
             &Client::makeServiceHandle,
             "Create a ServiceHandle instance",
             "address"_a,
             "provider_id"_a=0)
        .def("make_service_group_handle",
             [](const Client& client,
                const std::string& groupfile,
                uint16_t provider_id) {
                return client.makeServiceGroupHandle(groupfile, provider_id);
             },
             "Create a ServiceHandle instance",
             "group_file"_a,
             "provider_id"_a=0)
        .def("make_service_group_handle",
             [](const Client& client,
                const std::vector<std::string>& addresses,
                uint16_t provider_id) {
                return client.makeServiceGroupHandle(addresses, provider_id);
             },
             "Create a ServiceHandle instance",
             "addresses"_a,
             "provider_id"_a=0)
        .def("make_service_group_handle",
             [](const Client& client,
                uint64_t gid,
                uint16_t provider_id) {
                return client.makeServiceGroupHandle(gid, provider_id);
             },
             "Create a ServiceHandle instance",
             "group_id"_a,
             "provider_id"_a=0);
    py11::class_<ServiceHandle>(m, "ServiceHandle")
        .def_property_readonly("client", &ServiceHandle::client)
        .def_property_readonly("address", [](const ServiceHandle& sh) {
                return static_cast<std::string>(sh.providerHandle());
            })
        .def_property_readonly("provider_id", [](const ServiceHandle& sh) {
                return sh.providerHandle().provider_id();
            })
        .def("get_config",
             [](const ServiceHandle& sh) {
                std::string config;
                sh.getConfig(&config);
                return config;
             })
        .def("query_config",
            [](const ServiceHandle& sh, const std::string& script) {
                std::string result;
                sh.queryConfig(script, &result);
                return result;
            }, "script"_a)
        .def("load_module",
            [](const ServiceHandle& sh,
               const std::string& name,
               const std::string& path) {
                    sh.loadModule(name, path);
            }, "name"_a, "path"_a)
        .def("add_provider",
            [](const ServiceHandle& sh,
               const std::string& description) {
                    uint16_t provider_id_out;
                    sh.addProvider(description, &provider_id_out);
                    return provider_id_out;
            }, "description"_a)
        .def("change_provider_pool",
             [](const ServiceHandle& sh,
                const std::string& provider_name,
                const std::string& pool_name) {
                sh.changeProviderPool(provider_name, pool_name);
             })
        .def("add_client",
            [](const ServiceHandle& sh,
               const std::string& description) {
                    sh.addClient(description);
            }, "description"_a)
        .def("add_abtio_instance",
            [](const ServiceHandle& sh,
               const std::string& description) {
                    sh.addABTioInstance(description);
            }, "description"_a = std::string("{}"))
        .def("add_ssg_group", [](const ServiceHandle& sh, const std::string& config) {
                sh.addSSGgroup(config);
            },
            "description"_a)
        .def("add_pool", [](const ServiceHandle& sh, const std::string& config) {
                sh.addPool(config);
            },
            "description"_a)
        .def("remove_pool", [](const ServiceHandle& sh, const std::string& pool_name) {
                sh.removePool(pool_name);
            },
            "name"_a)
        .def("add_xstream", [](const ServiceHandle& sh, const std::string& config) {
                sh.addXstream(config);
            },
            "description"_a)
        .def("remove_xstream", [](const ServiceHandle& sh, const std::string& es_name) {
                sh.removeXstream(es_name);
            },
            "name"_a)
    ;
    py11::class_<ServiceGroupHandle>(m, "ServiceGroupHandle")
        .def("refresh", &ServiceGroupHandle::refresh)
        .def_property_readonly("size", &ServiceGroupHandle::size)
        .def_property_readonly("client", &ServiceGroupHandle::client)
        .def("__getitem__", &ServiceGroupHandle::operator[])
        .def("get_config",
             [](const ServiceGroupHandle& sh) {
                std::string config;
                sh.getConfig(&config);
                return config;
             })
        .def("query_config",
            [](const ServiceGroupHandle& sh, const std::string& script) {
                std::string result;
                sh.queryConfig(script, &result);
                return result;
            }, "script"_a)
    ;
#endif
}
