/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "types.h"
#include "client.h"
#include "flock/flock-group-view.h"
#include "flock/flock-client.h"
#include <json-c/json.h>

flock_return_t flock_group_handle_create(
        flock_client_t client,
        hg_addr_t addr,
        uint16_t provider_id,
        uint32_t mode,
        flock_group_handle_t* handle)
{
    if(client == FLOCK_CLIENT_NULL)
        return FLOCK_ERR_INVALID_ARGS;

    hg_return_t hret;

    char buffer[128];
    memset(buffer, 0, 128);
    size_t bufsize = 128;
    hret = margo_provider_get_identity(client->mid, addr, provider_id, buffer, &bufsize);
    if(hret != HG_SUCCESS) {
        margo_error(client->mid,
            "[flock] Could not create group handle, "
            "margo_provider_get_identity failed: %s",
            HG_Error_to_string(hret));
        return FLOCK_ERR_INVALID_PROVIDER;
    }
    if(strcmp("flock", buffer) != 0) {
        margo_error(client->mid,
            "[flock] Could not create group handle, "
            "provider %hu is not a flock provider, but a %s provider",
            provider_id, buffer);
        return FLOCK_ERR_INVALID_PROVIDER;
    }

    flock_group_handle_t rh =
        (flock_group_handle_t)calloc(1, sizeof(*rh));

    if(!rh) return FLOCK_ERR_ALLOCATION;

    hret = margo_addr_dup(client->mid, addr, &(rh->addr));
    if(hret != HG_SUCCESS) {
        margo_error(client->mid,
            "[flock] Could not create group handle, "
            "margo_addr_dup failed: %s",
            HG_Error_to_string(hret));
        free(rh);
        return FLOCK_ERR_FROM_MERCURY;
    }

    rh->client      = client;
    rh->provider_id = provider_id;
    rh->refcount    = 1;

    client->num_group_handles += 1;

    if(mode & FLOCK_MODE_INIT_UPDATE) {
        flock_return_t ret = flock_group_update_view(rh, NULL);
        if(ret != FLOCK_SUCCESS) {
            flock_group_handle_release(rh);
            return ret;
        }
    } else {
        char address[256];
        hg_size_t address_size = 256;
        hret = margo_addr_to_string(client->mid, address, &address_size, addr);
        if(hret != HG_SUCCESS) {
            margo_error(client->mid,
                "[flock] Could not create group handle, "
                "margo_addr_to_string failed: %s",
                HG_Error_to_string(hret));
            flock_group_handle_release(rh);
            return FLOCK_ERR_FROM_MERCURY;
        }
        flock_group_view_add_member(&rh->view, address, provider_id);
    }

    *handle = rh;
    return FLOCK_SUCCESS;
}

flock_return_t flock_group_handle_ref_incr(
        flock_group_handle_t handle)
{
    if(handle == FLOCK_GROUP_HANDLE_NULL)
        return FLOCK_ERR_INVALID_ARGS;
    handle->refcount += 1;
    return FLOCK_SUCCESS;
}

flock_return_t flock_group_handle_release(flock_group_handle_t handle)
{
    if(handle == FLOCK_GROUP_HANDLE_NULL)
        return FLOCK_ERR_INVALID_ARGS;
    handle->refcount -= 1;
    if(handle->refcount == 0) {
        margo_addr_free(handle->client->mid, handle->addr);
        flock_group_view_clear(&handle->view);
        handle->client->num_group_handles -= 1;
        free(handle);
    }
    return FLOCK_SUCCESS;
}

static inline flock_return_t group_handle_create_from_view(
        flock_client_t client,
        flock_group_view_t* view,
        uint32_t mode,
        uint64_t credentials,
        flock_group_handle_t* handle)
{
    flock_group_handle_t rh =
        (flock_group_handle_t)calloc(1, sizeof(*rh));

    if(!rh) return FLOCK_ERR_ALLOCATION;

    const char* addr = view->members.data[0].address;

    hg_return_t hret = margo_addr_lookup(client->mid, addr, &(rh->addr));
    if(hret != HG_SUCCESS) return FLOCK_ERR_FROM_MERCURY;

    rh->client      = client;
    rh->provider_id = view->members.data[0].provider_id;
    rh->refcount    = 1;
    rh->credentials = credentials;
    FLOCK_GROUP_VIEW_MOVE(view, &rh->view);

    client->num_group_handles += 1;

    if(mode & FLOCK_MODE_INIT_UPDATE) {
        flock_return_t ret = flock_group_update_view(rh, NULL);
        if(ret != FLOCK_SUCCESS) {
            flock_group_handle_release(rh);
            return ret;
        }
    }

    *handle = rh;
    return FLOCK_SUCCESS;
}

flock_return_t flock_group_handle_create_from_file(
        flock_client_t client,
        const char* filename,
        uint32_t mode,
        flock_group_handle_t* handle)
{
    if(client == FLOCK_CLIENT_NULL)
        return FLOCK_ERR_INVALID_ARGS;

    flock_group_view_t view = FLOCK_GROUP_VIEW_INITIALIZER;
    uint64_t credentials = 0;
    flock_return_t ret = flock_group_view_from_file(filename, &view);
    if(ret != FLOCK_SUCCESS) return ret;

    ret = group_handle_create_from_view(client, &view, mode, credentials, handle);
    if(ret != FLOCK_SUCCESS)
        flock_group_view_clear(&view);

    return ret;
}

flock_return_t flock_group_handle_create_from_serialized(
        flock_client_t client,
        const char* serialized_view,
        size_t view_size,
        uint32_t mode,
        flock_group_handle_t* handle)
{
    if(client == FLOCK_CLIENT_NULL)
        return FLOCK_ERR_INVALID_ARGS;

    flock_group_view_t view = FLOCK_GROUP_VIEW_INITIALIZER;
    uint64_t credentials = 0;
    flock_return_t ret = flock_group_view_from_string(
        serialized_view, view_size, &view);
    if(ret != FLOCK_SUCCESS) return ret;

    flock_group_handle_t rh =
        (flock_group_handle_t)calloc(1, sizeof(*rh));

    if(!rh) return FLOCK_ERR_ALLOCATION;

    ret = group_handle_create_from_view(client, &view, mode, credentials, handle);
    if(ret != FLOCK_SUCCESS)
        flock_group_view_clear(&view);

    return ret;
}

flock_return_t flock_group_get_view(
        flock_group_handle_t handle,
        flock_group_view_t* view)
{
    FLOCK_GROUP_VIEW_LOCK(&handle->view);
    flock_group_view_clear(view);
    view->digest            = handle->view.digest;
    view->members.size      = handle->view.members.size;
    view->members.capacity  = handle->view.members.capacity;
    view->members.data      = calloc(sizeof(*view->members.data), view->members.size);
    for(size_t i = 0; i < view->members.size; ++i) {
        view->members.data[i].address     = strdup(handle->view.members.data[i].address);
        view->members.data[i].provider_id = handle->view.members.data[i].provider_id;
    }
    view->metadata.size     = handle->view.metadata.size;
    view->metadata.capacity = handle->view.metadata.capacity;
    view->metadata.data     = calloc(sizeof(*view->metadata.data), view->metadata.size);
    for(size_t i = 0; i < view->metadata.size; ++i) {
        view->metadata.data[i].key = strdup(handle->view.metadata.data[i].key);
        view->metadata.data[i].value = strdup(handle->view.metadata.data[i].value);
    }
    FLOCK_GROUP_VIEW_UNLOCK(&handle->view);
    return FLOCK_SUCCESS;
}

flock_return_t flock_group_access_view(
        flock_group_handle_t handle,
        void (*access)(void* uargs, const flock_group_view_t* view),
        void* uargs)
{
    FLOCK_GROUP_VIEW_LOCK(&handle->view);
    if (access) {
        access(uargs, &handle->view);
    }
    FLOCK_GROUP_VIEW_UNLOCK(&handle->view);
    return FLOCK_SUCCESS;
}

flock_return_t flock_group_metadata_set(
          flock_group_handle_t handle,
          const char* key,
          const char* value)
{
    // TODO
    (void)handle;
    (void)key;
    (void)value;
    return FLOCK_ERR_OP_UNSUPPORTED;
}
