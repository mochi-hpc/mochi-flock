/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py11 = pybind11;
using namespace pybind11::literals;

struct margo_instance;
typedef struct margo_instance* margo_instance_id;
typedef struct hg_addr* hg_addr_t;
typedef py11::capsule pymargo_instance_id;
typedef py11::capsule pyhg_addr_t;

#define MID2CAPSULE(__mid)   py11::capsule((void*)(__mid), "margo_instance_id")
#define CAPSULE2MID(__caps)  (margo_instance_id)(__caps)

#define ADDR2CAPSULE(__addr)   py11::capsule((void*)(__addr), "hg_addr_t")
#define CAPSULE2ADDR(__caps)  (hg_add_t)(__caps)

PYBIND11_MODULE(pybedrock_server, m) {
    m.doc() = "Flock server python extension";
#if 0
    py11::register_exception<Exception>(m, "Exception", PyExc_RuntimeError);

    py11::class_<NamedDependency, std::shared_ptr<NamedDependency>> named_dep(m, "NamedDependency");
    named_dep
        .def_property_readonly("name",
            [](std::shared_ptr<NamedDependency> nd) { return nd->getName(); })
        .def_property_readonly("type",
            [](std::shared_ptr<NamedDependency> nd) { return nd->getType(); })
        .def_property_readonly("handle",
            [](std::shared_ptr<NamedDependency> nd) { return nd->getHandle<intptr_t>(); })
    ;

    py11::class_<ProviderDependency, std::shared_ptr<ProviderDependency>> (m, "ProviderDependency", named_dep)
        .def_property_readonly("provider_id",
            [](std::shared_ptr<ProviderDependency> nd) { return nd->getProviderID(); })
    ;

    py11::class_<Server, std::shared_ptr<Server>> server(m, "Server");

    py11::enum_<ConfigType>(server, "ConfigType")
    .value("JSON", ConfigType::JSON)
    .value("JX9", ConfigType::JX9)
    .export_values();

    server
        .def(py11::init([](const std::string& address, const std::string& config,
                           ConfigType configType,
                           const Jx9ParamMap& jx9Params) {
            return std::make_shared<Server>(address, config, configType, jx9Params);
        }), "address"_a, "config"_a="{}", "config_type"_a=ConfigType::JSON,
            "jx9_params"_a=Jx9ParamMap{})
        .def("wait_for_finalize",
             [](std::shared_ptr<Server> server) {
                server->waitForFinalize();
             })
        .def("finalize",
             [](std::shared_ptr<Server> server) {
                server->finalize();
             })
        .def_property_readonly("margo_instance_id",
             [](std::shared_ptr<Server> server) {
                return MID2CAPSULE(server->getMargoManager().getMargoInstance());
             })
        .def_property_readonly("config",
             [](std::shared_ptr<Server> server) {
                return server->getCurrentConfig();
             })
        .def_property_readonly("margo_manager",
             [](std::shared_ptr<Server> server) {
                return server->getMargoManager();
             })
        .def_property_readonly("abtio_manager",
             [](std::shared_ptr<Server> server) {
                return server->getABTioManager();
             })
        .def_property_readonly("provider_manager",
             [](std::shared_ptr<Server> server) {
                return server->getProviderManager();
             })
        .def_property_readonly("client_manager",
             [](std::shared_ptr<Server> server) {
                return server->getClientManager();
             })
        .def_property_readonly("ssg_manager",
             [](std::shared_ptr<Server> server) {
                return server->getSSGManager();
             })
    ;

    py11::class_<MargoManager> (m, "MargoManager")
        .def_property_readonly("margo_instance_id", [](const MargoManager& m) {
                return MID2CAPSULE(m.getMargoInstance());
        })
        .def_property_readonly("default_handler_pool", &MargoManager::getDefaultHandlerPool)
        .def_property_readonly("config", &MargoManager::getCurrentConfig)
        .def("get_pool", [](const MargoManager& m, const std::string& pool_name) {
                return m.getPool(pool_name);
        }, "name"_a)
        .def("get_pool", [](const MargoManager& m, uint32_t index) {
                return m.getPool(index);
        }, "index"_a)
        .def("add_pool", &MargoManager::addPool,
             "config"_a)
        .def("remove_pool", [](MargoManager& m, const std::string& pool_name) {
                return m.removePool(pool_name);
        }, "name"_a)
        .def("remove_pool", [](MargoManager& m, uint32_t index) {
                return m.removePool(index);
        }, "index"_a)
        .def_property_readonly("num_pools", &MargoManager::getNumPools)
        .def("get_xstream", [](const MargoManager& m, const std::string& es_name) {
                return m.getXstream(es_name);
        }, "name"_a)
        .def("get_xstream", [](const MargoManager& m, uint32_t index) {
                return m.getXstream(index);
        }, "index"_a)
        .def("add_xstream", &MargoManager::addXstream,
             "config"_a)
        .def("remove_xstream", [](MargoManager& m, const std::string& es_name) {
                return m.removeXstream(es_name);
        }, "name"_a)
        .def("remove_xstream", [](MargoManager& m, uint32_t index) {
                return m.removeXstream(index);
        }, "index"_a)
        .def_property_readonly("num_xstreams", &MargoManager::getNumXstreams)
    ;

    py11::class_<SSGManager> (m, "SSGManager")
        .def_property_readonly("config", [](const SSGManager& manager) {
            return manager.getCurrentConfig().dump();
        })
        .def_property_readonly("num_groups", &SSGManager::getNumGroups)
        .def("get_group", [](const SSGManager& ssg, const std::string& name) {
                return ssg.getGroup(name);
             }, "name_a")
        .def("get_group", [](const SSGManager& ssg, size_t index) {
                return ssg.getGroup(index);
             }, "index_a")
        .def("add_group",
             [](SSGManager& ssg,
                const std::string& name,
                const py11::dict& config,
                const std::shared_ptr<NamedDependency>& pool,
                const std::string& bootstrap_method,
                const std::string& group_file,
                int64_t credential) {
#ifdef ENABLE_SSG
                ssg_group_config_t cfg = SSG_GROUP_CONFIG_INITIALIZER;
                cfg.ssg_credential = credential;
#define GET_SSG_FIELD(__field__) do { \
                if(config.contains(#__field__)) \
                    cfg.swim_##__field__ = config[#__field__].cast<decltype(cfg.swim_##__field__)>(); \
                } while(0)
                GET_SSG_FIELD(period_length_ms);
                GET_SSG_FIELD(suspect_timeout_periods);
                GET_SSG_FIELD(subgroup_member_count);
                GET_SSG_FIELD(disabled);
#undef GET_SSG_FIELD
                return ssg.addGroup(name, cfg, pool, bootstrap_method, group_file);
#else
                throw Exception{"Bedrock was not compiled with SSG support"};
#endif
             }, "name"_a, "swim"_a=py11::dict{},
                "pool"_a=nullptr, "bootstrap"_a="init",
                "group_file"_a="", "credential"_a=-1)
        .def("resolve_address", [](const SSGManager& ssg, const std::string& address) {
                return ADDR2CAPSULE(ssg.resolveAddress(address));
            }, "address"_a)
    ;

    py11::class_<ABTioManager> (m, "ABTioManager")
        .def_property_readonly("config", [](const ABTioManager& manager) {
            return manager.getCurrentConfig().dump();
        })
        .def_property_readonly("num_abtio_instances", &ABTioManager::numABTioInstances)
        .def("get_abtio_instance", [](const ABTioManager& abtio, const std::string& name) {
            return abtio.getABTioInstance(name);
        }, "name"_a)
        .def("get_abtio_instance", [](const ABTioManager& abtio, size_t index) {
            return abtio.getABTioInstance(index);
        }, "index"_a)
        .def("add_abtio_instance", &ABTioManager::addABTioInstance)
    ;

    py11::class_<ProviderManager> (m, "ProviderManager")
        .def_property_readonly("config", &ProviderManager::getCurrentConfig)
        .def_property_readonly("num_providers", &ProviderManager::numProviders)
        .def("get_provider", [](const ProviderManager& pm, size_t index) {
                return pm.getProvider(index);
             }, "index"_a)
        .def("get_provider", [](const ProviderManager& pm, const std::string& name) {
                return pm.getProvider(name);
             }, "name"_a)
        .def("lookup_provider", &ProviderManager::lookupProvider,
             "spec"_a)
        .def("deregister_provider",
             &ProviderManager::deregisterProvider,
             "spec"_a)
        .def("add_provider",
             &ProviderManager::addProviderFromJSON,
             "description"_a)
        .def("change_pool",
             &ProviderManager::changeProviderPool,
             "provider"_a, "pool"_a)
        .def("migrate_provider",
             &ProviderManager::migrateProvider,
             "provider"_a, "dest_addr"_a, "dest_provider_id"_a,
             "migration_config"_a, "remove_source"_a)
        .def("snapshot_provider",
             &ProviderManager::snapshotProvider,
             "provider"_a, "dest_path"_a, "snapshot_config"_a, "remove_source"_a)
        .def("restore_provider",
             &ProviderManager::restoreProvider,
             "provider"_a, "src_path"_a, "restore_config"_a)
    ;

    py11::class_<ClientManager> (m, "ClientManager")
        .def_property_readonly("config", &ClientManager::getCurrentConfig)
        .def("get_client", [](const ClientManager& cm, const std::string& name) {
                return cm.getClient(name);
             },
             "name"_a)
        .def("get_client", [](const ClientManager& cm, size_t index) {
                return cm.getClient(index);
             },
             "index"_a)
        .def_property_readonly("num_clients", &ClientManager::numClients)
        .def("remove_client", [](ClientManager& cm, const std::string& name) {
                return cm.removeClient(name);
             },
             "name"_a)
        .def("remove_client", [](ClientManager& cm, size_t index) {
                return cm.removeClient(index);
             },
             "index"_a)
        .def("get_client_or_create", &ClientManager::getOrCreateAnonymous,
             "type"_a)
        .def("add_client", &ClientManager::addClientFromJSON,
             "description"_a)
    ;
#endif
}
