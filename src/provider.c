/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "alpha/alpha-server.h"
#include "provider.h"
#include "types.h"

/* backends that we want to add at compile time */
#include "dummy/dummy-backend.h"
/* Note: other backends can be added dynamically using
 * alpha_register_backend */

static void alpha_finalize_provider(void* p);

/* global array of up to 64 registered backends */
#define ALPHA_MAX_NUM_BACKENDS 64
static alpha_backend_impl* g_alpha_backend_types[ALPHA_MAX_NUM_BACKENDS] = {0};

/* Functions to manipulate the list of backend types */
static inline alpha_backend_impl* find_backend_impl(const char* name);
static inline alpha_return_t add_backend_impl(alpha_backend_impl* backend);

/* Client RPCs */
static DECLARE_MARGO_RPC_HANDLER(alpha_sum_ult)
static void alpha_sum_ult(hg_handle_t h);

/* FIXME: add other RPC declarations here */

alpha_return_t alpha_provider_register(
        margo_instance_id mid,
        uint16_t provider_id,
        const char* config_str,
        const struct alpha_provider_args* args,
        alpha_provider_t* provider)
{
    struct alpha_provider_args a = ALPHA_PROVIDER_ARGS_INIT;
    if(args) a = *args;
    alpha_provider_t p;
    hg_id_t id;
    hg_bool_t flag;
    alpha_return_t ret = ALPHA_SUCCESS;

    margo_info(mid, "Registering ALPHA provider with provider id %u", provider_id);

    /* check if another provider with the same ID is already registered */
    if(margo_provider_registered_identity(mid, provider_id)) {
        margo_error(mid, "A provider with the same ID is already registered");
        return ALPHA_ERR_INVALID_PROVIDER;
    }

    flag = margo_is_listening(mid);
    if(flag == HG_FALSE) {
        margo_error(mid, "Margo instance is not a server");
        return ALPHA_ERR_INVALID_ARGS;
    }

    // parse json configuration
    struct json_object* config = NULL;
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
        if (!(json_object_is_type(config, json_type_object))) {
            margo_error(mid, "JSON configuration should be an object");
            json_object_put(config);
            return ALPHA_ERR_INVALID_CONFIG;
        }
    } else {
        // create default JSON config
        config = json_object_new_object();
    }

    p = (alpha_provider_t)calloc(1, sizeof(*p));
    if(p == NULL) {
        margo_error(mid, "Could not allocate memory for provider");
        json_object_put(config);
        return ALPHA_ERR_ALLOCATION;
    }

    p->mid = mid;
    p->provider_id = provider_id;
    p->pool = a.pool;

    /* Client RPCs */

    id = MARGO_REGISTER_PROVIDER(mid, "alpha_sum",
            sum_in_t, sum_out_t,
            alpha_sum_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->sum_id = id;

    /* FIXME: add other RPC registration here */
    /* ... */

    /* add backends available at compiler time (e.g. default/dummy backends) */
    alpha_register_dummy_backend(); // function from "dummy/dummy-backend.h"
    /* FIXME: add other backend registrations here */
    /* ... */

    /* read the configuration to add defined resources */
    struct json_object* resource = json_object_object_get(config, "resource");
    if (resource) {
        if (!json_object_is_type(resource, json_type_object)) {
            margo_error(mid, "\"resource\" field should be an object in provider configuration");
            ret = ALPHA_ERR_INVALID_CONFIG;
            goto finish;
        }
        struct json_object* resource_type = json_object_object_get(resource, "type");
        if (!json_object_is_type(resource_type, json_type_string)) {
            margo_error(mid, "\"type\" field in resource configuration should be a string");
            ret = ALPHA_ERR_INVALID_CONFIG;
            goto finish;
        }
        const char* type = json_object_get_string(resource_type);
        alpha_backend_impl* backend         = find_backend_impl(type);
        if (!backend) {
            margo_error(mid, "Could not find backend of type \"%s\"", type);
            ret = ALPHA_ERR_INVALID_CONFIG;
            goto finish;
        }
        struct json_object* resource_config = json_object_object_get(resource, "config");
        /* create the new resource's context */
        void* context = NULL;
        ret = backend->create_resource(
            mid, p,
            resource_config ? json_object_to_json_string(resource_config) : "{}",
            &context);
        if (ret != ALPHA_SUCCESS) {
            margo_error(mid, "Could not create resource, backend returned %d", ret);
            goto finish;
        }

        /* set the provider's resource */
        p->resource = malloc(sizeof(*(p->resource)));
        p->resource->ctx = context;
        p->resource->fn  = backend;
    }


    /* set the finalize callback */
    margo_provider_push_finalize_callback(mid, p, &alpha_finalize_provider, p);

    /* set the provider's identity */
    margo_provider_register_identity(mid, provider_id, "alpha");

    if(provider)
        *provider = p;

    margo_info(mid, "ALPHA provider registration done");

finish:
    if(config) json_object_put(config);
    return ret;
}

static void alpha_finalize_provider(void* p)
{
    alpha_provider_t provider = (alpha_provider_t)p;
    margo_info(provider->mid, "Finalizing ALPHA provider");
    margo_provider_deregister_identity(provider->mid, provider->provider_id);
    margo_deregister(provider->mid, provider->create_resource_id);
    margo_deregister(provider->mid, provider->destroy_resource_id);
    margo_deregister(provider->mid, provider->sum_id);
    /* FIXME deregister other RPC ids ... */

    /* destroy the resource's context */
    if(provider->resource)
        provider->resource->fn->destroy_resource(provider->resource->ctx);
    free(provider->resource);
    margo_instance_id mid = provider->mid;
    free(provider);
    margo_info(mid, "ALPHA provider successfuly finalized");
}

alpha_return_t alpha_provider_destroy(
        alpha_provider_t provider)
{
    margo_instance_id mid = provider->mid;
    margo_info(mid, "Destroying ALPHA provider");
    /* pop the finalize callback */
    margo_provider_pop_finalize_callback(provider->mid, provider);
    /* call the callback */
    alpha_finalize_provider(provider);
    margo_info(mid, "ALPHA provider successfuly destroyed");
    return ALPHA_SUCCESS;
}

char* alpha_provider_get_config(alpha_provider_t provider)
{
    if (!provider) return NULL;
    struct json_object* root = json_object_new_object();
    if(provider->resource) {
        struct json_object* resource = json_object_new_object();
        json_object_object_add(root, "resource", resource);
        struct json_object* resource_type = json_object_new_string(provider->resource->fn->name);
        json_object_object_add(resource, "type", resource_type);
        char* resource_config_str = (provider->resource->fn->get_config)(provider->resource->ctx);
        struct json_object* resource_config = json_tokener_parse(resource_config_str);
        free(resource_config_str);
        json_object_object_add(resource, "config", resource_config);
    }
    char* result = strdup(json_object_to_json_string(root));
    json_object_put(root);
    return result;
}

alpha_return_t alpha_provider_register_backend(
        alpha_backend_impl* backend_impl)
{
    margo_info(MARGO_INSTANCE_NULL, "Adding backend implementation \"%s\" to ALPHA",
               backend_impl->name);
    return add_backend_impl(backend_impl);
}

static void alpha_sum_ult(hg_handle_t h)
{
    hg_return_t hret;
    sum_in_t     in;
    sum_out_t   out;

    /* find the margo instance */
    margo_instance_id mid = margo_hg_handle_get_instance(h);

    /* find the provider */
    const struct hg_info* info = margo_get_info(h);
    alpha_provider_t provider = (alpha_provider_t)margo_registered_data(mid, info->id);

    /* deserialize the input */
    hret = margo_get_input(h, &in);
    if(hret != HG_SUCCESS) {
        margo_error(mid, "Could not deserialize output (mercury error %d)", hret);
        out.ret = ALPHA_ERR_FROM_MERCURY;
        goto finish;
    }

    alpha_resource* resource = provider->resource;
    if(!resource) {
        out.ret = ALPHA_ERR_INVALID_RESOURCE;
        goto finish;
    }

    /* call sum on the resource's context */
    out.result = resource->fn->sum(resource->ctx, in.x, in.y);
    out.ret = ALPHA_SUCCESS;

    margo_debug(mid, "Called sum RPC");

finish:
    hret = margo_respond(h, &out);
    hret = margo_free_input(h, &in);
    margo_destroy(h);
}
static DEFINE_MARGO_RPC_HANDLER(alpha_sum_ult)

static inline alpha_backend_impl* find_backend_impl(const char* name)
{
    for(size_t i = 0; i < ALPHA_MAX_NUM_BACKENDS; i++) {
        alpha_backend_impl* impl = g_alpha_backend_types[i];
        if(impl == NULL) return NULL;
        if(strcmp(name, impl->name) == 0)
            return impl;
    }
    return NULL;
}

static inline alpha_return_t add_backend_impl(
        alpha_backend_impl* backend)
{
    if(find_backend_impl(backend->name)) return ALPHA_SUCCESS;
    for(size_t i = 0; i < ALPHA_MAX_NUM_BACKENDS; i++) {
        if(g_alpha_backend_types[i]) continue;
        g_alpha_backend_types[i] = backend;
        return ALPHA_SUCCESS;
    }
    return ALPHA_ERR_ALLOCATION;
}

alpha_return_t alpha_register_backend(alpha_backend_impl* backend_impl)
{
    return add_backend_impl(backend_impl);
}
