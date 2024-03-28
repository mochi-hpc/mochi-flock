/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <string.h>
#include <json-c/json.h>
#include "flock/flock-backend.h"
#include "flock/flock-group-view.h"
#include "flock/flock-client.h"
#include "flock/flock-group.h"
#include "../provider.h"
#include "centralized-backend.h"

typedef struct centralized_context {
    struct json_object*  config;
    ABT_mutex_memory     config_mtx;
    bool                 is_primary;
    flock_group_view_t   view;
    flock_client_t       client;
    flock_group_handle_t this_group;
} centralized_context;

static flock_return_t centralized_destroy_group(void* ctx);

static flock_return_t centralized_create_group(
        flock_backend_init_args_t* args,
        void** context)
{
    if(args->initial_view.members.size == 0)
        return FLOCK_ERR_INVALID_ARGS;

    centralized_context* ctx = (centralized_context*)calloc(1, sizeof(*ctx));
    if(!ctx) return FLOCK_ERR_ALLOCATION;

    margo_instance_id mid = args->mid;
    hg_addr_t self_address;
    char      self_address_string[256];
    hg_size_t self_address_size = 256;
    hg_return_t hret = margo_addr_self(mid, &self_address);
    hret = margo_addr_to_string(mid, self_address_string, &self_address_size, self_address);
    margo_addr_free(mid, self_address);

    flock_member_t* primary_member = &args->initial_view.members.data[0];
    ctx->is_primary = (primary_member->provider_id == args->provider_id)
                   && (strcmp(primary_member->address, self_address_string) == 0);

    FLOCK_GROUP_VIEW_MOVE(&args->initial_view, &ctx->view);

    flock_return_t ret = flock_client_init(mid, args->pool, &ctx->client);
    if(ret != FLOCK_SUCCESS) {
        centralized_destroy_group(ctx);
        return ret;
    }
    hg_addr_t primary_address = HG_ADDR_NULL;
    hret = margo_addr_lookup(mid, primary_member->address, &primary_address);
    if(hret != HG_SUCCESS) {
        centralized_destroy_group(ctx);
        return FLOCK_ERR_FROM_MERCURY;
    }
    ret = flock_group_handle_create(
            ctx->client,
            primary_address,
            primary_member->provider_id,
            0, &ctx->this_group);
    margo_addr_free(mid, primary_address);
    if(ret != FLOCK_SUCCESS) {
        centralized_destroy_group(ctx);
        return ret;
    }

    ctx->config = json_object_new_object();

    *context = ctx;
    return FLOCK_SUCCESS;
}

static flock_return_t centralized_destroy_group(void* ctx)
{
    centralized_context* context = (centralized_context*)ctx;
    if(context->config)
        json_object_put(context->config);
    if(context->is_primary) {
        flock_group_view_clear(&context->view);
    } else {
        if(context->this_group) flock_group_handle_release(context->this_group);
        if(context->client) flock_client_finalize(context->client);
    }
    free(context);
    return FLOCK_SUCCESS;
}

static flock_return_t centralized_get_config(
    void* ctx, void (*fn)(void*, const struct json_object*), void* uargs)
{
    centralized_context* context = (centralized_context*)ctx;
    ABT_mutex_lock(ABT_MUTEX_MEMORY_GET_HANDLE(&context->config_mtx));
    fn(uargs, context->config);
    ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&context->config_mtx));
    return FLOCK_SUCCESS;
}

static flock_return_t centralized_get_view(
    void* ctx, void (*fn)(void*, const flock_group_view_t* view), void* uargs)
{
    centralized_context* context = (centralized_context*)ctx;
    fn(uargs, &context->view);
    return FLOCK_SUCCESS;
}

static flock_return_t centralized_add_member(
    void* ctx, uint64_t rank, const char* address, uint16_t provider_id)
{
    // LCOV_EXCL_START
    (void)ctx;
    (void)rank;
    (void)address;
    (void)provider_id;
    return FLOCK_ERR_OP_UNSUPPORTED;
    // LCOV_EXCL_END
}

static flock_return_t centralized_remove_member(
    void* ctx, uint64_t rank)
{
    // LCOV_EXCL_START
    (void)ctx;
    (void)rank;
    return FLOCK_ERR_OP_UNSUPPORTED;
    // LCOV_EXCL_END
}

static flock_return_t centralized_add_metadata(
    void* ctx, const char* key, const char* value)
{
    // LCOV_EXCL_START
    (void)ctx;
    (void)key;
    (void)value;
    return FLOCK_ERR_OP_UNSUPPORTED;
    // LCOV_EXCL_END
}

static flock_return_t centralized_remove_metadata(
    void* ctx, const char* key)
{
    // LCOV_EXCL_START
    (void)ctx;
    (void)key;
    return FLOCK_ERR_OP_UNSUPPORTED;
    // LCOV_EXCL_END
}

static flock_backend_impl centralized_backend = {
    .name               = "centralized",
    .init_group         = centralized_create_group,
    .destroy_group      = centralized_destroy_group,
    .get_config         = centralized_get_config,
    .get_view           = centralized_get_view,
    .add_member         = centralized_add_member,
    .remove_member      = centralized_remove_member,
    .add_metadata       = centralized_add_metadata,
    .remove_metadata    = centralized_remove_metadata
};

flock_return_t flock_register_centralized_backend(void)
{
    return flock_register_backend(&centralized_backend);
}
