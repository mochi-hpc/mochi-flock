/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <string.h>
#include <json-c/json.h>
#include <flock/flock-common.h>
#include "flock/flock-gateway.h"
#include "default-gateway.h"

typedef struct default_gateway_context {
    struct json_object* config;
    char*               public_address_str;
} default_gateway_context;

static flock_return_t default_gateway_create(
        flock_gateway_init_args_t* args,
        void** context)
{

    default_gateway_context* ctx = (default_gateway_context*)calloc(1, sizeof(*ctx));
    if(!ctx) return FLOCK_ERR_ALLOCATION;

    ctx->config = json_object_new_object();
    ctx->public_address_str = calloc(256, 1);

    hg_addr_t self_addr = HG_ADDR_NULL;
    hg_size_t addr_str_size = 256;

    margo_addr_self(args->mid, &self_addr);
    margo_addr_to_string(args->mid, ctx->public_address_str, &addr_str_size, self_addr);
    margo_addr_free(args->mid, self_addr);

    *context = ctx;
    return FLOCK_SUCCESS;
}

static flock_return_t default_gateway_destroy(void* ctx)
{
    default_gateway_context* context = (default_gateway_context*)ctx;
    json_object_put(context->config);
    free(context->public_address_str);
    free(context);
    return FLOCK_SUCCESS;
}

static flock_return_t default_gateway_get_config(
    void* ctx, void (*fn)(void*, const struct json_object*), void* uargs)
{
    default_gateway_context* context = (default_gateway_context*)ctx;
    fn(uargs, context->config);
    return FLOCK_SUCCESS;
}

static const char* default_gateway_get_public_address(void* ctx)
{
    default_gateway_context* context = (default_gateway_context*)ctx;
    return context ? context->public_address_str : NULL;
}

static flock_gateway_impl default_gateway = {
    .name               = "default",
    .init_gateway       = default_gateway_create,
    .destroy_gateway    = default_gateway_destroy,
    .get_config         = default_gateway_get_config,
    .get_public_address = default_gateway_get_public_address
};

flock_return_t flock_register_default_gateway(void)
{
    return flock_register_gateway(&default_gateway);
}
