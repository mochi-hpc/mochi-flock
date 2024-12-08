/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "flock/flock-server.h"
#include "flock/flock-bootstrap.h"
#ifdef ENABLE_MPI
#include "flock/flock-bootstrap-mpi.h"
#endif
#include <bedrock/AbstractComponent.hpp>
#include <iostream>
#include <json-c/json.h>

class FlockComponent : public bedrock::AbstractComponent {

    flock_provider_t m_provider = FLOCK_PROVIDER_NULL;

    public:

    FlockComponent(flock_provider_t p)
    : m_provider{p} {}

    ~FlockComponent() {
        flock_provider_destroy(m_provider);
    }

    static std::shared_ptr<bedrock::AbstractComponent> Register(const bedrock::ComponentArgs& args) {

        flock_return_t ret;
        auto mid = args.engine.get_margo_instance();
        auto provider_id = args.provider_id;
        auto pool_dep_it = args.dependencies.find("pool");
        thallium::pool pool;
        if(pool_dep_it != args.dependencies.end()) {
            pool = pool_dep_it->second[0]->getHandle<thallium::pool>();
        } else {
            pool = args.engine.get_handler_pool();
        }

        flock_group_view_t initial_view = FLOCK_GROUP_VIEW_INITIALIZER;

        flock_provider_args flock_args = FLOCK_PROVIDER_ARGS_INIT;
        flock_args.pool = pool.native_handle();
        flock_args.initial_view = &initial_view;

        struct json_tokener* tokener = json_tokener_new();
        struct json_object* config = json_tokener_parse_ex(
                tokener, args.config.c_str(), args.config.size());
        json_tokener_free(tokener);
        auto _delete_config = std::shared_ptr<json_object>{config, json_object_put};

        if(!(json_object_is_type(config, json_type_object)))
            throw bedrock::Exception{
                "[flock] JSON provider configuration should be an object"};

        json_object* bootstrap = json_object_object_get(config, "bootstrap");
        if(!bootstrap)
            throw bedrock::Exception{
                "[flock] \"bootstrap\" field not found in provider configuration"};

        if(!json_object_is_type(bootstrap, json_type_string)
        && !json_object_is_type(bootstrap, json_type_array))
            throw bedrock::Exception{
                "[flock] \"bootstrap\" field should be of type string or array"};

        if(json_object_is_type(bootstrap, json_type_array)) {
            for(size_t i = 0; i < json_object_array_length(bootstrap); ++i) {
                if(!json_object_is_type(json_object_array_get_idx(bootstrap, i), json_type_string)) {
                    throw bedrock::Exception{
                        "[flock] \"bootstrap\" array should only contain strings"};
                }
            }
            json_object_get(bootstrap);
        } else {
            auto tmp = json_object_new_array();
            json_object_array_add(tmp,
                    json_object_new_string(json_object_get_string(bootstrap)));
            bootstrap = tmp;
        }
        auto _delete_bootstrap = std::shared_ptr<json_object>{bootstrap, json_object_put};

        json_object* mpi_ranks = json_object_object_get(config, "mpi_ranks");
        if(mpi_ranks && !(json_object_is_type(mpi_ranks, json_type_array)))
            throw bedrock::Exception{"[flock] \"mpi_ranks\" field should be an array"};

        std::vector<int> ranks;
        if(mpi_ranks) {
            ranks.resize(json_object_array_length(mpi_ranks));
            for(size_t i = 0; i < ranks.size(); ++i) {
                json_object* rank = json_object_array_get_idx(mpi_ranks, i);
                if(!json_object_is_type(rank, json_type_int))
                    throw bedrock::Exception{
                        "[flock] \"mpi_ranks\" should contain only integers"};
                ranks[i] = json_object_get_int64(rank);
            }
        }

        for(size_t method_idx = 0; method_idx < json_object_array_length(bootstrap); ++method_idx) {

            const char* bootstrap_str = json_object_get_string(
                    json_object_array_get_idx(bootstrap, method_idx));

            try {

                flock_args.initial_view = &initial_view;
                // --------------------- self ----------------------- //
                if(strcmp(bootstrap_str, "self") == 0) {
                    ret = flock_group_view_init_from_self(mid, provider_id, &initial_view);
                    if(ret != FLOCK_SUCCESS)
                        throw bedrock::Exception{
                            "[flock] Failed to initialized group with flock_group_view_init_from_self"};
                // --------------------- mpi ------------------------ //
                } else if(strcmp(bootstrap_str, "mpi") == 0) {
#if ENABLE_MPI
                    int mpi_initialized = 0;
                    MPI_Initialized(&mpi_initialized);
                    if(!mpi_initialized)
                        MPI_Init(NULL, NULL);
                    MPI_Comm comm = MPI_COMM_WORLD;
                    int world_size;
                    MPI_Comm_size(comm, &world_size);
                    if(mpi_ranks) {
                        for(size_t i = 0; i < ranks.size(); ++i) {
                            if(ranks[i] >= 0 && ranks[i] < world_size) continue;
                            throw bedrock::Exception{"[flock] Invalid rank in \"mpi_ranks\" list"};
                        }
                        MPI_Group world_group;
                        MPI_Comm_group(MPI_COMM_WORLD, &world_group);
                        MPI_Group flock_group;
                        MPI_Group_incl(world_group, ranks.size(), ranks.data(), &flock_group);
                        MPI_Comm_create_group(MPI_COMM_WORLD, flock_group, 0, &comm);
                        MPI_Group_free(&flock_group);
                        MPI_Group_free(&world_group);
                    }

                    ret = flock_group_view_init_from_mpi(mid, provider_id, comm, &initial_view);

                    if(mpi_ranks) MPI_Comm_free(&comm);
                    if(ret != FLOCK_SUCCESS)
                        throw bedrock::Exception{
                            "[flock] Failed to initialized group with flock_group_view_init_from_mpi"};
#else
                    throw bedrock::Exception{"[flock] Flock was not built with MPI support"};
#endif
                // --------------------- join ----------------------- //
                } else if(strcmp(bootstrap_str, "join") == 0) {
                    struct json_object* filename = json_object_object_get(config, "file");
                    if(!filename || !json_object_is_type(filename, json_type_string))
                        throw bedrock::Exception{
                            "[flock] \"file\" field not found (or is not a string) "
                                "required to join the group"};
                    const char* filename_str = json_object_get_string(filename);
                    ret = flock_group_view_init_from_file(filename_str, &initial_view);
                    if(ret != FLOCK_SUCCESS)
                        throw bedrock::Exception{
                            "[flock] Failed to initialized group with flock_group_view_init_from_file"};
                } else {
                    throw bedrock::Exception{
                        std::string{"[flock] Invalid value \""}
                        + bootstrap_str + "\" for \"bootstrap\" field"};
                }

            } catch(const bedrock::Exception& ex) {
                if(method_idx == json_object_array_length(bootstrap)-1)
                    throw;
                spdlog::warn(ex.what());
                continue;
            }

            break; // if we reached this point, the current method worked
        }

        flock_provider_t provider = FLOCK_PROVIDER_NULL;
        ret = flock_provider_register(
                args.engine.get_margo_instance(),
                args.provider_id,
                args.config.c_str(),
                &flock_args,
                &provider);
        if(ret != FLOCK_SUCCESS)
            throw bedrock::Exception{
                std::string{"[flock] Could not register provider (flock_provider_register returned "}
                + std::to_string((int)ret) + ")"};

        return std::make_shared<FlockComponent>(provider);
    }

    static std::vector<bedrock::Dependency>
        GetDependencies(const bedrock::ComponentArgs& args) {
        (void)args;
        std::vector<bedrock::Dependency> deps = {
            { "pool", "pool", false, false, false }
        };
        return deps;
    }

    void* getHandle() override {
        return static_cast<void*>(m_provider);
    }
};

BEDROCK_REGISTER_COMPONENT_TYPE(flock, FlockComponent)
