/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "flock/flock-server.h"
#include "provider.h"
#include "types.h"

/* backends that we want to add at compile time */
#include "static/static-backend.h"
/* Note: other backends can be added dynamically using
 * flock_register_backend */

static void flock_finalize_provider(void* p);

/* global array of up to 64 registered backends */
#define FLOCK_MAX_NUM_BACKENDS 64
static flock_backend_impl* g_flock_backend_types[FLOCK_MAX_NUM_BACKENDS] = {0};

/* Functions to manipulate the list of backend types */
static inline flock_backend_impl* find_backend_impl(const char* name);
static inline flock_return_t add_backend_impl(flock_backend_impl* backend);

/* Functions to dispatch updates to user-supplied callback functions */
static void dispatch_member_update(
    void* p, flock_update_t u, size_t rank, const char* address, uint16_t provider_id);
static void dispatch_metadata_update(
    void* p, const char* key, const char* value);

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

    ABT_rwlock_create(&p->update_callbacks_lock);
    p->update_callbacks = NULL;

    /* Client RPCs */

    id = MARGO_REGISTER_PROVIDER(mid, "flock_update",
            void, update_out_t,
            flock_update_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->update_id = id;

    /* FIXME: add other RPC registration here */
    /* ... */

    /* add backends available at compiler time (e.g. default/static backends) */
    flock_register_static_backend(); // function from "static/static-backend.h"
    /* FIXME: add other backend registrations here */
    /* ... */

    /* read the configuration to add defined groups */
    struct json_object* group = json_object_object_get(config, "group");
    struct json_object* group_config = NULL;
    if (group) {
        if (!json_object_is_type(group, json_type_object)) {
            margo_error(mid, "[flock] \"group\" field should be an object in provider configuration");
            ret = FLOCK_ERR_INVALID_CONFIG;
            goto finish;
        }
        if(!a.backend) {
            struct json_object* group_type = json_object_object_get(group, "type");
            if (!json_object_is_type(group_type, json_type_string)) {
                margo_error(mid, "[flock] \"type\" field in group configuration should be a string");
                ret = FLOCK_ERR_INVALID_CONFIG;
                goto finish;
            }
            const char* type = json_object_get_string(group_type);
            a.backend = find_backend_impl(type);
            if (!a.backend) {
                margo_error(mid, "[flock] Could not find backend of type \"%s\"", type);
                ret = FLOCK_ERR_INVALID_CONFIG;
                goto finish;
            }
        } else if(json_object_object_get(group, "type")) {
            margo_warning(mid, "[flock] \"type\" field ignored because a "
                          "backend implementation was provided");
        }
        group_config = json_object_object_get(group, "config");
        if(group_config) json_object_get(group_config);
    }

    if(!a.backend) {
        margo_error(mid, "[flock] No backend provided for the group");
        ret = FLOCK_ERR_INVALID_CONFIG;
        goto finish;
    }

    if(!group_config) group_config = json_object_new_object();

    /* create the new group's context */
    void* context = NULL;
    flock_backend_init_args_t backend_init_args = {
        .mid = mid,
        .pool = p->pool,
        .provider_id = provider_id,
        .config = group_config,
        .initial_view = FLOCK_GROUP_VIEW_INITIALIZER,
        .callback_context = p,
        .member_update_callback = dispatch_member_update,
        .metadata_update_callback = dispatch_metadata_update
    };

    if(a.initial_view)
        FLOCK_GROUP_VIEW_MOVE(a.initial_view, &backend_init_args.initial_view);

    ret = a.backend->init_group(&backend_init_args, &context);
    if (ret != FLOCK_SUCCESS) {
        margo_error(mid, "[flock] Could not create group, backend returned %d", ret);
        goto finish;
    }

    /* set the provider's group */
    p->group = malloc(sizeof(*(p->group)));
    p->group->ctx = context;
    p->group->fn  = a.backend;

    /* set the finalize callback */
    margo_provider_push_finalize_callback(mid, p, &flock_finalize_provider, p);

    /* set the provider's identity */
    margo_provider_register_identity(mid, provider_id, "flock");

    if(provider)
        *provider = p;

    margo_info(mid, "[flock] Provider registered with ID %d", (int)provider_id);

finish:
    if(config) json_object_put(config);
    if(group_config) json_object_put(group_config);
    return ret;
}

static void flock_finalize_provider(void* p)
{
    flock_provider_t provider = (flock_provider_t)p;
    margo_info(provider->mid, "[flock] Finalizing provider");

    ABT_rwlock_free(&provider->update_callbacks_lock);
    update_callback_t u = provider->update_callbacks;
    update_callback_t tmp;
    while(u) {
        tmp = u;
        u = u->next;
        free(tmp);
    }

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

static inline void get_backend_config(void* uargs, const struct json_object* config) {
    struct json_object* root = (struct json_object*)uargs;
    struct json_object* config_cpy;
    json_object_deep_copy((struct json_object*)config, &config_cpy,  NULL);
    json_object_object_add(root, "config", config_cpy);
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
        (provider->group->fn->get_config)(provider->group->ctx, get_backend_config, (void*)root);
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

flock_return_t flock_provider_add_update_callbacks(
        flock_provider_t provider,
        flock_membership_update_fn member_update_fn,
        flock_metadata_update_fn metadata_update_fn,
        void* context)
{
    ABT_rwlock_wrlock(provider->update_callbacks_lock);
    // Append at the end or with the same context if found
    // (this will replace any membership/metadata callback already
    // registered with the same context).
    update_callback_t current = provider->update_callbacks;
    while(true) {
        if(current->args == context) {
            current->member_cb   = member_update_fn;
            current->metadata_cb = metadata_update_fn;
            break;
        } else if(current->next == NULL) {
            current->next = (update_callback_t)malloc(sizeof(*current->next));
            current->next->args = context;
            current->next->next = NULL;
            // callbacks will be set in the next loop
        }
        current = current->next;
    }
    ABT_rwlock_unlock(provider->update_callbacks_lock);
    return FLOCK_SUCCESS;
}

flock_return_t flock_provider_remove_update_callbacks(
        flock_provider_t provider,
        void* context)
{
    ABT_rwlock_wrlock(provider->update_callbacks_lock);
    update_callback_t current = provider->update_callbacks;
    update_callback_t previous = NULL;
    while(true) {
        if(current->args == context) {
            if(!previous) {
                provider->update_callbacks = current->next;
            } else {
                previous->next = current->next;
            }
            free(current);
            break;
        }
        previous = current;
        current  = current->next;
    }
    ABT_rwlock_unlock(provider->update_callbacks_lock);
    return FLOCK_SUCCESS;
}

static void dispatch_member_update(
    void* p, flock_update_t u, size_t rank, const char* address, uint16_t provider_id)
{
    flock_provider_t provider = (flock_provider_t)p;
    ABT_rwlock_rdlock(provider->update_callbacks_lock);
    update_callback_t c = provider->update_callbacks;
    while(c) {
        (c->member_cb)(c->args, u, rank, address, provider_id);
        c = c->next;
    }
    ABT_rwlock_unlock(provider->update_callbacks_lock);
}

static void dispatch_metadata_update(
    void* p, const char* key, const char* value)
{
    flock_provider_t provider = (flock_provider_t)p;
    ABT_rwlock_rdlock(provider->update_callbacks_lock);
    update_callback_t c = provider->update_callbacks;
    while(c) {
        (c->metadata_cb)(c->args, key, value);
        c = c->next;
    }
    ABT_rwlock_unlock(provider->update_callbacks_lock);
}
