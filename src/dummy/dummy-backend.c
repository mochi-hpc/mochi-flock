/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <string.h>
#include <json-c/json.h>
#include "alpha/alpha-backend.h"
#include "../provider.h"
#include "dummy-backend.h"

typedef struct dummy_context {
    struct json_object* config;
    /* ... */
} dummy_context;

static alpha_return_t dummy_create_resource(
        margo_instance_id mid,
        alpha_provider_t provider,
        const char* config_str,
        void** context)
{
    (void)provider;
    struct json_object* config = NULL;

    // read JSON config from provided string argument
    if (config_str) {
        struct json_tokener*    tokener = json_tokener_new();
        enum json_tokener_error jerr;
        config = json_tokener_parse_ex(
                tokener, config_str,
                strlen(config_str));
        if (!config) {
            jerr = json_tokener_get_error(tokener);
            margo_error(mid, "JSON parse error: %s",
                      json_tokener_error_desc(jerr));
            json_tokener_free(tokener);
            return ALPHA_ERR_INVALID_CONFIG;
        }
        json_tokener_free(tokener);
    } else {
        // create default JSON config
        config = json_object_new_object();
    }

    dummy_context* ctx = (dummy_context*)calloc(1, sizeof(*ctx));
    ctx->config = config;
    *context = (void*)ctx;
    return ALPHA_SUCCESS;
}

static alpha_return_t dummy_destroy_resource(void* ctx)
{
    dummy_context* context = (dummy_context*)ctx;
    json_object_put(context->config);
    free(context);
    return ALPHA_SUCCESS;
}

static char* dummy_get_config(void* ctx)
{
    (void)ctx;
    return strdup("{}");
}

static int32_t dummy_compute_sum(void* ctx, int32_t x, int32_t y)
{
    (void)ctx;
    return x+y;
}

static alpha_backend_impl dummy_backend = {
    .name             = "dummy",

    .create_resource  = dummy_create_resource,
    .destroy_resource = dummy_destroy_resource,
    .get_config       = dummy_get_config,

    .sum              = dummy_compute_sum
};

alpha_return_t alpha_register_dummy_backend(void)
{
    return alpha_register_backend(&dummy_backend);
}
