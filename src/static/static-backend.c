/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <string.h>
#include <json-c/json.h>
#include "flock/flock-backend.h"
#include "flock/flock-group-view.h"
#include "../provider.h"
#include "static-backend.h"

typedef struct static_context {
    struct json_object* config;
    ABT_mutex_memory    config_mtx;
    flock_group_view_t  view;
} static_context;

static flock_return_t static_create_group(
        flock_backend_init_args_t* args,
        void** context)
{
    static_context* ctx = (static_context*)calloc(1, sizeof(*ctx));
    if(!ctx) return FLOCK_ERR_ALLOCATION;

    ctx->config = json_object_new_object();
    FLOCK_GROUP_VIEW_MOVE(&args->initial_view, &ctx->view);

    *context = ctx;
    return FLOCK_SUCCESS;
}

static flock_return_t static_destroy_group(void* ctx)
{
    static_context* context = (static_context*)ctx;
    json_object_put(context->config);
    flock_group_view_clear(&context->view);
    free(context);
    return FLOCK_SUCCESS;
}

static flock_return_t static_get_config(
    void* ctx, void (*fn)(void*, const struct json_object*), void* uargs)
{
    static_context* context = (static_context*)ctx;
    ABT_mutex_lock(ABT_MUTEX_MEMORY_GET_HANDLE(&context->config_mtx));
    fn(uargs, context->config);
    ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&context->config_mtx));
    return FLOCK_SUCCESS;
}

static flock_return_t static_get_view(
    void* ctx, void (*fn)(void*, const flock_group_view_t* view), void* uargs)
{
    static_context* context = (static_context*)ctx;
    fn(uargs, &context->view);
    return FLOCK_SUCCESS;
}

static flock_return_t static_add_member(
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

static flock_return_t static_remove_member(
    void* ctx, uint64_t rank)
{
    // LCOV_EXCL_START
    (void)ctx;
    (void)rank;
    return FLOCK_ERR_OP_UNSUPPORTED;
    // LCOV_EXCL_END
}

static flock_return_t static_add_metadata(
    void* ctx, const char* key, const char* value)
{
    // LCOV_EXCL_START
    (void)ctx;
    (void)key;
    (void)value;
    return FLOCK_ERR_OP_UNSUPPORTED;
    // LCOV_EXCL_END
}

static flock_return_t static_remove_metadata(
    void* ctx, const char* key)
{
    // LCOV_EXCL_START
    (void)ctx;
    (void)key;
    return FLOCK_ERR_OP_UNSUPPORTED;
    // LCOV_EXCL_END
}

static flock_backend_impl static_backend = {
    .name               = "static",
    .init_group         = static_create_group,
    .destroy_group      = static_destroy_group,
    .get_config         = static_get_config,
    .get_view           = static_get_view,
    .add_member         = static_add_member,
    .remove_member      = static_remove_member,
    .add_metadata       = static_add_metadata,
    .remove_metadata    = static_remove_metadata
};

flock_return_t flock_register_static_backend(void)
{
    return flock_register_backend(&static_backend);
}
