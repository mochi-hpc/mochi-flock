/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <string.h>
#include <json-c/json.h>
#include "flock/flock-backend.h"
#include "../provider.h"
#include "dummy-backend.h"

typedef struct dummy_context {
    struct json_object* config;
    /* ... */
} dummy_context;

static flock_return_t dummy_create_group(
        const flock_backend_init_args_t* args,
        void** context)
{
    dummy_context* ctx = (dummy_context*)calloc(1, sizeof(*ctx));
    json_object_deep_copy(args->config, &ctx->config, NULL);
    *context = ctx;
    return FLOCK_SUCCESS;
}

static flock_return_t dummy_destroy_group(void* ctx)
{
    dummy_context* context = (dummy_context*)ctx;
    json_object_put(context->config);
    free(context);
    return FLOCK_SUCCESS;
}

static char* dummy_get_config(void* ctx)
{
    dummy_context* context = (dummy_context*)ctx;
    return strdup(json_object_to_json_string(context->config));
}

static int32_t dummy_compute_sum(void* ctx, int32_t x, int32_t y)
{
    (void)ctx;
    return x+y;
}

static flock_backend_impl dummy_backend = {
    .name          = "dummy",

    .init_group    = dummy_create_group,
    .destroy_group = dummy_destroy_group,
    .get_config    = dummy_get_config,

    .sum           = dummy_compute_sum
};

flock_return_t flock_register_dummy_backend(void)
{
    return flock_register_backend(&dummy_backend);
}
