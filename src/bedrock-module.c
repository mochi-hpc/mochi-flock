/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <bedrock/module.h>
#include "alpha/alpha-server.h"
#include "alpha/alpha-client.h"
#include "client.h"
#include <string.h>

static int alpha_register_provider(
        bedrock_args_t args,
        bedrock_module_provider_t* provider)
{
    margo_instance_id mid = bedrock_args_get_margo_instance(args);
    uint16_t provider_id  = bedrock_args_get_provider_id(args);

    struct alpha_provider_args alpha_args = { 0 };
    const char* config = bedrock_args_get_config(args);
    alpha_args.pool   = bedrock_args_get_pool(args);

    return alpha_provider_register(mid, provider_id, config, &alpha_args,
                                   (alpha_provider_t*)provider);
}

static int alpha_deregister_provider(
        bedrock_module_provider_t provider)
{
    return alpha_provider_destroy((alpha_provider_t)provider);
}

static char* alpha_get_provider_config(
        bedrock_module_provider_t provider) {
    (void)provider;
    return alpha_provider_get_config(provider);
}

static int alpha_init_client(
        bedrock_args_t args,
        bedrock_module_client_t* client)
{
    margo_instance_id mid = bedrock_args_get_margo_instance(args);
    return alpha_client_init(mid, (alpha_client_t*)client);
}

static int alpha_finalize_client(
        bedrock_module_client_t client)
{
    return alpha_client_finalize((alpha_client_t)client);
}

static char* alpha_get_client_config(
        bedrock_module_client_t client) {
    (void)client;
    // TODO
    return strdup("{}");
}

static int alpha_create_provider_handle(
        bedrock_module_client_t client,
        hg_addr_t address,
        uint16_t provider_id,
        bedrock_module_provider_handle_t* ph)
{
    alpha_client_t c = (alpha_client_t)client;
    alpha_resource_handle_t tmp;
    alpha_resource_handle_create(c, address, provider_id, true, &tmp);
    *ph = (bedrock_module_provider_handle_t)tmp;
    return BEDROCK_SUCCESS;
}

static int alpha_destroy_provider_handle(
        bedrock_module_provider_handle_t ph)
{
    alpha_resource_handle_t tmp = (alpha_resource_handle_t)ph;
    alpha_resource_handle_release(tmp);
    return BEDROCK_SUCCESS;
}

static struct bedrock_module alpha = {
    .register_provider       = alpha_register_provider,
    .deregister_provider     = alpha_deregister_provider,
    .get_provider_config     = alpha_get_provider_config,
    .init_client             = alpha_init_client,
    .finalize_client         = alpha_finalize_client,
    .get_client_config       = alpha_get_client_config,
    .create_provider_handle  = alpha_create_provider_handle,
    .destroy_provider_handle = alpha_destroy_provider_handle,
    .provider_dependencies   = NULL,
    .client_dependencies     = NULL
};

BEDROCK_REGISTER_MODULE(alpha, alpha)
