/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "flock/flock-server.h"
#include "provider.h"
#include "types.h"

/* backends that we want to add at compile time */
#include "dummy/dummy-backend.h"
/* Note: other backends can be added dynamically using
 * flock_register_backend */

static void flock_finalize_provider(void* p);

/* global array of up to 64 registered backends */
#define FLOCK_MAX_NUM_BACKENDS 64
static flock_backend_impl* g_flock_backend_types[FLOCK_MAX_NUM_BACKENDS] = {0};

/* Functions to manipulate the list of backend types */
static inline flock_backend_impl* find_backend_impl(const char* name);
static inline flock_return_t add_backend_impl(flock_backend_impl* backend);

/* Client RPCs */
static DECLARE_MARGO_RPC_HANDLER(flock_update_ult)
static void flock_update_ult(hg_handle_t h);

/* FIXME: add other RPC declarations here */

flock_return_t flock_provider_register(
        margo_instance_id mid,
        uint16_t provider_id,
        const char* config_str,
        const struct flock_provider_args* args,
        flock_provider_t* provider)
{
    struct flock_provider_args a = FLOCK_PROVIDER_ARGS_INIT;
    if(args) a = *args;
    flock_provider_t p;
    hg_id_t id;
    hg_bool_t flag;
    flock_return_t ret = FLOCK_SUCCESS;

    margo_info(mid, "Registering FLOCK provider with provider id %u", provider_id);

    /* check if another provider with the same ID is already registered */
    if(margo_provider_registered_identity(mid, provider_id)) {
        margo_error(mid, "A provider with the same ID is already registered");
        return FLOCK_ERR_INVALID_PROVIDER;
    }

    flag = margo_is_listening(mid);
    if(flag == HG_FALSE) {
        margo_error(mid, "Margo instance is not a server");
        return FLOCK_ERR_INVALID_ARGS;
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
            return FLOCK_ERR_INVALID_CONFIG;
        }
        json_tokener_free(tokener);
        if (!(json_object_is_type(config, json_type_object))) {
            margo_error(mid, "JSON configuration should be an object");
            json_object_put(config);
            return FLOCK_ERR_INVALID_CONFIG;
        }
    } else {
        // create default JSON config
        config = json_object_new_object();
    }

    p = (flock_provider_t)calloc(1, sizeof(*p));
    if(p == NULL) {
        margo_error(mid, "Could not allocate memory for provider");
        json_object_put(config);
        return FLOCK_ERR_ALLOCATION;
    }

    p->mid = mid;
    p->provider_id = provider_id;
    p->pool = a.pool;

    /* Client RPCs */

    id = MARGO_REGISTER_PROVIDER(mid, "flock_update",
            void, update_out_t,
            flock_update_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->update_id = id;

    /* FIXME: add other RPC registration here */
    /* ... */

    /* add backends available at compiler time (e.g. default/dummy backends) */
    flock_register_dummy_backend(); // function from "dummy/dummy-backend.h"
    /* FIXME: add other backend registrations here */
    /* ... */

    /* read the configuration to add defined groups */
    struct json_object* group = json_object_object_get(config, "group");
    if (group) {
        if (!json_object_is_type(group, json_type_object)) {
            margo_error(mid, "\"group\" field should be an object in provider configuration");
            ret = FLOCK_ERR_INVALID_CONFIG;
            goto finish;
        }
        struct json_object* group_type = json_object_object_get(group, "type");
        if (!json_object_is_type(group_type, json_type_string)) {
            margo_error(mid, "\"type\" field in group configuration should be a string");
            ret = FLOCK_ERR_INVALID_CONFIG;
            goto finish;
        }
        const char* type = json_object_get_string(group_type);
        flock_backend_impl* backend         = find_backend_impl(type);
        if (!backend) {
            margo_error(mid, "Could not find backend of type \"%s\"", type);
            ret = FLOCK_ERR_INVALID_CONFIG;
            goto finish;
        }
        struct json_object* group_config = json_object_object_get(group, "config");

        /* create the new group's context */
        void* context = NULL;
        flock_backend_init_args_t init_args = {
            .mid = mid,
            .pool = p->pool,
            .provider_id = provider_id,
            .config = group_config
        };
        ret = backend->init_group(&init_args, &context);
        if (ret != FLOCK_SUCCESS) {
            margo_error(mid, "Could not create group, backend returned %d", ret);
            goto finish;
        }

        /* set the provider's group */
        p->group = malloc(sizeof(*(p->group)));
        p->group->ctx = context;
        p->group->fn  = backend;
    }


    /* set the finalize callback */
    margo_provider_push_finalize_callback(mid, p, &flock_finalize_provider, p);

    /* set the provider's identity */
    margo_provider_register_identity(mid, provider_id, "flock");

    if(provider)
        *provider = p;

    margo_info(mid, "[flock] Provider registered with ID %d", (int)provider_id);

finish:
    if(config) json_object_put(config);
    return ret;
}

static void flock_finalize_provider(void* p)
{
    flock_provider_t provider = (flock_provider_t)p;
    margo_info(provider->mid, "[flock] Finalizing provider");
    margo_provider_deregister_identity(provider->mid, provider->provider_id);
    margo_deregister(provider->mid, provider->update_id);
    /* FIXME deregister other RPC ids ... */

    /* destroy the group's context */
    if(provider->group)
        provider->group->fn->destroy_group(provider->group->ctx);
    free(provider->group);
    margo_instance_id mid = provider->mid;
    free(provider);
    margo_info(mid, "[flock] Provider successfuly finalized");
}

flock_return_t flock_provider_destroy(
        flock_provider_t provider)
{
    margo_instance_id mid = provider->mid;
    margo_info(mid, "[flock] Destroying provider");
    /* pop the finalize callback */
    margo_provider_pop_finalize_callback(provider->mid, provider);
    /* call the callback */
    flock_finalize_provider(provider);
    margo_info(mid, "[flock] Provider successfuly destroyed");
    return FLOCK_SUCCESS;
}

char* flock_provider_get_config(flock_provider_t provider)
{
    if (!provider) return NULL;
    struct json_object* root = json_object_new_object();
    if(provider->group) {
        struct json_object* group = json_object_new_object();
        json_object_object_add(root, "group", group);
        struct json_object* group_type = json_object_new_string(provider->group->fn->name);
        json_object_object_add(group, "type", group_type);
        char* group_config_str = (provider->group->fn->get_config)(provider->group->ctx);
        struct json_object* group_config = json_tokener_parse(group_config_str);
        free(group_config_str);
        json_object_object_add(group, "config", group_config);
    }
    char* result = strdup(json_object_to_json_string(root));
    json_object_put(root);
    return result;
}

flock_return_t flock_provider_register_backend(
        flock_backend_impl* backend_impl)
{
    margo_info(MARGO_INSTANCE_NULL, "Adding backend implementation \"%s\" to FLOCK",
               backend_impl->name);
    return add_backend_impl(backend_impl);
}

static void flock_update_ult(hg_handle_t h)
{
    hg_return_t hret;
    update_out_t   out;

    /* find the margo instance */
    margo_instance_id mid = margo_hg_handle_get_instance(h);

    /* find the provider */
    const struct hg_info* info = margo_get_info(h);
    flock_provider_t provider = (flock_provider_t)margo_registered_data(mid, info->id);

    flock_group* group = provider->group;
    if(!group) {
        out.ret = FLOCK_ERR_INVALID_GROUP;
        goto finish;
    }

    /* call update on the group's context */
//    out.result = group->fn->update(group->ctx, in.x, in.y);
    out.ret = FLOCK_SUCCESS;

    margo_debug(mid, "Called update RPC");

finish:
    hret = margo_respond(h, &out);
    margo_destroy(h);
}
static DEFINE_MARGO_RPC_HANDLER(flock_update_ult)

static inline flock_backend_impl* find_backend_impl(const char* name)
{
    for(size_t i = 0; i < FLOCK_MAX_NUM_BACKENDS; i++) {
        flock_backend_impl* impl = g_flock_backend_types[i];
        if(impl == NULL) return NULL;
        if(strcmp(name, impl->name) == 0)
            return impl;
    }
    return NULL;
}

static inline flock_return_t add_backend_impl(
        flock_backend_impl* backend)
{
    if(find_backend_impl(backend->name)) return FLOCK_SUCCESS;
    for(size_t i = 0; i < FLOCK_MAX_NUM_BACKENDS; i++) {
        if(g_flock_backend_types[i]) continue;
        g_flock_backend_types[i] = backend;
        return FLOCK_SUCCESS;
    }
    return FLOCK_ERR_ALLOCATION;
}

flock_return_t flock_register_backend(flock_backend_impl* backend_impl)
{
    return add_backend_impl(backend_impl);
}
