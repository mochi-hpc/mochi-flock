/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <string.h>
#include <stdlib.h>
#include <json-c/json.h>
#include <margo-timer.h>
#include "flock/flock-backend.h"
#include "flock/flock-group-view.h"
#include "../provider.h"
#include "swim-backend.h"

static flock_return_t swim_create_group(
        flock_backend_init_args_t* args,
        void** context)
{
    // LCOV_EXCL_START
    (void)args;
    (void)context;
    return FLOCK_ERR_OP_UNSUPPORTED;
    // LCOV_EXCL_STOP
}

static flock_return_t swim_destroy_group(void* ctx)
{
    // LCOV_EXCL_START
    (void)ctx;
    return FLOCK_ERR_OP_UNSUPPORTED;
    // LCOV_EXCL_STOP
}

static flock_return_t swim_get_config(
    void* ctx, void (*fn)(void*, const struct json_object*), void* uargs)
{
    // LCOV_EXCL_START
    (void)ctx;
    (void)fn;
    (void)uargs;
    return FLOCK_ERR_OP_UNSUPPORTED;
    // LCOV_EXCL_STOP
}

static flock_return_t swim_get_view(
    void* ctx, void (*fn)(void*, const flock_group_view_t* view), void* uargs)
{
    // LCOV_EXCL_START
    (void)ctx;
    (void)fn;
    (void)uargs;
    return FLOCK_ERR_OP_UNSUPPORTED;
    // LCOV_EXCL_STOP
}

static flock_return_t swim_add_metadata(
        void* ctx, const char* key, const char* value)
{
    // LCOV_EXCL_START
    (void)ctx;
    (void)key;
    (void)value;
    return FLOCK_ERR_OP_UNSUPPORTED;
    // LCOV_EXCL_STOP
}

static flock_return_t swim_remove_metadata(
        void* ctx, const char* key)
{
    // LCOV_EXCL_START
    (void)ctx;
    (void)key;
    return FLOCK_ERR_OP_UNSUPPORTED;
    // LCOV_EXCL_STOP
}

static flock_backend_impl swim_backend = {
    .name            = "swim",
    .init_group      = swim_create_group,
    .destroy_group   = swim_destroy_group,
    .get_config      = swim_get_config,
    .get_view        = swim_get_view,
    .add_metadata    = swim_add_metadata,
    .remove_metadata = swim_remove_metadata
};

flock_return_t flock_register_swim_backend(void)
{
    return flock_register_backend(&swim_backend);
}
