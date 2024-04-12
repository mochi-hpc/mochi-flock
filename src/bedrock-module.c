/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <bedrock/module.h>
#include "flock/flock-server.h"
#include "flock/flock-client.h"
#include "client.h"
#include <string.h>

static int flock_register_provider(
        bedrock_args_t args,
        bedrock_module_provider_t* provider)
{
    margo_instance_id mid = bedrock_args_get_margo_instance(args);
    uint16_t provider_id  = bedrock_args_get_provider_id(args);

    struct flock_provider_args flock_args = FLOCK_PROVIDER_ARGS_INIT;
    const char* config = bedrock_args_get_config(args);
    flock_args.pool   = bedrock_args_get_pool(args);

    // TODO find a way to specify the initial view
    return flock_provider_register(mid, provider_id, config, &flock_args,
                                    (flock_provider_t*)provider);
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
