/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <bedrock/module.h>
#include "flock/flock-server.h"
#include "flock/flock-client.h"
#include "flock/flock-bootstrap.h"
#ifdef ENABLE_MPI
#include "flock/flock-bootstrap-mpi.h"
#endif
#include "client.h"
#include <json-c/json.h>
#include <string.h>

static int flock_register_provider(
        bedrock_args_t args,
        bedrock_module_provider_t* provider)
{
    flock_return_t ret = FLOCK_SUCCESS;

    margo_instance_id mid = bedrock_args_get_margo_instance(args);
    uint16_t provider_id  = bedrock_args_get_provider_id(args);

    struct flock_provider_args flock_args = FLOCK_PROVIDER_ARGS_INIT;
    const char* config_str = bedrock_args_get_config(args);
    flock_args.pool   = bedrock_args_get_pool(args);

    flock_group_view_t initial_view = FLOCK_GROUP_VIEW_INITIALIZER;
    int* ranks = NULL;

    struct json_tokener* tokener = json_tokener_new();
    struct json_object* config = json_tokener_parse_ex(
            tokener, config_str,
            strlen(config_str));
    json_tokener_free(tokener);

    if(!(json_object_is_type(config, json_type_object))) {
        margo_error(mid, "[flock] JSON provider configuration should be an object");
        ret = FLOCK_ERR_INVALID_CONFIG;
        goto finish;
    }

    struct json_object* bootstrap = json_object_object_get(config, "bootstrap");
    if(!bootstrap) {
        margo_error(mid, "[flock] \"bootstrap\" field not found in provider configuration");
        ret = FLOCK_ERR_INVALID_CONFIG;
        goto finish;
    }

    if(!json_object_is_type(bootstrap, json_type_string)) {
        margo_error(mid, "[flock] \"bootstrap\" field should be of type string");
        ret = FLOCK_ERR_INVALID_CONFIG;
        goto finish;
    }

    const char* bootstrap_str = json_object_get_string(bootstrap);

    struct json_object* mpi_ranks = json_object_object_get(config, "mpi_ranks");
    if(mpi_ranks && strcmp(bootstrap_str, "mpi") != 0) {
        margo_error(mid, "[flock] \"mpi_ranks\" field should only be provided for \"mpi\" bootstrap");
        ret = FLOCK_ERR_INVALID_CONFIG;
        goto finish;
    }

    if(mpi_ranks && !(json_object_is_type(mpi_ranks, json_type_array))) {
        margo_error(mid, "[flock] \"mpi_ranks\" field should be an array");
        ret = FLOCK_ERR_INVALID_CONFIG;
        goto finish;
    }

    int num_ranks = 0;
    if(mpi_ranks) {
        num_ranks = json_object_array_length(mpi_ranks);
        ranks = (int*)calloc(num_ranks, sizeof(int));
        for(int i = 0; i < num_ranks; ++i) {
            struct json_object* rank = json_object_array_get_idx(mpi_ranks, i);
            if(!json_object_is_type(rank, json_type_int)) {
                margo_error(mid, "[flock] \"mpi_ranks\" should contain only integers");
                ret = FLOCK_ERR_INVALID_CONFIG;
                goto finish;
            }
            ranks[i] = json_object_get_int64(rank);
        }
    }

    flock_args.initial_view = &initial_view;

    if(strcmp(bootstrap_str, "self") == 0) {
        ret = flock_group_view_init_from_self(mid, provider_id, &initial_view);
        if(ret != FLOCK_SUCCESS) goto finish;
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
            for(int i = 0; i < num_ranks; ++i) {
                if(ranks[i] >= 0 && ranks[i] < world_size) continue;
                margo_error(mid, "[flock] Invalid rank %d in \"mpi_ranks\" list", ranks[i]);
                ret = FLOCK_ERR_INVALID_CONFIG;
                goto finish;
            }
            MPI_Group world_group;
            MPI_Comm_group(MPI_COMM_WORLD, &world_group);
            MPI_Group flock_group;
            MPI_Group_incl(world_group, num_ranks, ranks, &flock_group);
            MPI_Comm_create_group(MPI_COMM_WORLD, flock_group, 0, &comm);
            MPI_Group_free(&flock_group);
            MPI_Group_free(&world_group);
        }

        ret = flock_group_view_init_from_mpi(mid, provider_id, comm, &initial_view);

        if(mpi_ranks) MPI_Comm_free(&comm);
        if(ret != FLOCK_SUCCESS) goto finish;
#else
        margo_error(mid, "[flock] Flock was not built with MPI support");
        json_object_put(config);
        return FLOCK_ERR_INVALID_CONFIG;
#endif
    } else if(strcmp(bootstrap_str, "join") == 0) {
        struct json_object* filename = json_object_object_get(config, "file");
        if(!filename || !json_object_is_type(filename, json_type_string)) {
            margo_error(mid, "[flock] \"file\" field not found (or is not a string) "
                             "required to join the group");
            json_object_put(config);
            return FLOCK_ERR_INVALID_CONFIG;
        }
        const char* filename_str = json_object_get_string(filename);
        ret = flock_group_view_init_from_file(filename_str, &initial_view);
        if(ret != FLOCK_SUCCESS) goto finish;
    } else {
        margo_error(mid, "[flock] Invalid value \"%s\" for \"bootstrap\" field", bootstrap_str);
        json_object_put(config);
        return FLOCK_ERR_INVALID_CONFIG;
    }

    ret = flock_provider_register(mid, provider_id, config_str, &flock_args,
                                  (flock_provider_t*)provider);

finish:
    flock_group_view_clear(&initial_view);
    json_object_put(config);
    free(ranks);
    return ret;
}

static int flock_deregister_provider(
        bedrock_module_provider_t provider)
{
    return flock_provider_destroy((flock_provider_t)provider);
}

static char* flock_get_provider_config(
        bedrock_module_provider_t provider) {
    (void)provider;
    return flock_provider_get_config(provider);
}

static int flock_init_client(
        bedrock_args_t args,
        bedrock_module_client_t* client)
{
    margo_instance_id mid = bedrock_args_get_margo_instance(args);
    return flock_client_init(mid, ABT_POOL_NULL, (flock_client_t*)client);
}

static int flock_finalize_client(
        bedrock_module_client_t client)
{
    return flock_client_finalize((flock_client_t)client);
}

static char* flock_get_client_config(
        bedrock_module_client_t client) {
    (void)client;
    // TODO
    return strdup("{}");
}

static int flock_create_provider_handle(
        bedrock_module_client_t client,
        hg_addr_t address,
        uint16_t provider_id,
        bedrock_module_provider_handle_t* ph)
{
    flock_client_t c = (flock_client_t)client;
    flock_group_handle_t tmp;
    flock_group_handle_create(c, address, provider_id, true, &tmp);
    *ph = (bedrock_module_provider_handle_t)tmp;
    return BEDROCK_SUCCESS;
}

static int flock_destroy_provider_handle(
        bedrock_module_provider_handle_t ph)
{
    flock_group_handle_t tmp = (flock_group_handle_t)ph;
    flock_group_handle_release(tmp);
    return BEDROCK_SUCCESS;
}

static struct bedrock_module flock = {
    .register_provider       = flock_register_provider,
    .deregister_provider     = flock_deregister_provider,
    .get_provider_config     = flock_get_provider_config,
    .init_client             = flock_init_client,
    .finalize_client         = flock_finalize_client,
    .get_client_config       = flock_get_client_config,
    .create_provider_handle  = flock_create_provider_handle,
    .destroy_provider_handle = flock_destroy_provider_handle,
    .provider_dependencies   = NULL,
    .client_dependencies     = NULL
};

BEDROCK_REGISTER_MODULE(flock, flock)
