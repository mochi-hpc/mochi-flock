/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "types.h"
#include "client.h"
#include "view-serialize.h"
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

    char buffer[sizeof("flock")];
    size_t bufsize = sizeof("flock");
    hret = margo_provider_get_identity(client->mid, addr, provider_id, buffer, &bufsize);
    if(hret != HG_SUCCESS || strcmp("flock", buffer) != 0)
        return FLOCK_ERR_INVALID_PROVIDER;

    flock_group_handle_t rh =
        (flock_group_handle_t)calloc(1, sizeof(*rh));

    if(!rh) return FLOCK_ERR_ALLOCATION;

    hret = margo_addr_dup(client->mid, addr, &(rh->addr));
    if(hret != HG_SUCCESS) {
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
            flock_group_handle_release(rh);
            return FLOCK_ERR_FROM_MERCURY;
        }
        flock_group_view_add_member(&rh->view, 0, provider_id, address);
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

flock_return_t flock_group_handle_create_from_file(
        flock_client_t client,
        const char* filename,
        uint32_t mode,
        flock_group_handle_t* handle)
{
    if(client == FLOCK_CLIENT_NULL)
        return FLOCK_ERR_INVALID_ARGS;

    // Read the content of the file into a buffer
    char* buffer = NULL;
    size_t length;
    FILE* file = fopen(filename, "r");
    if(!file) {
        margo_error(client->mid, "[flock] Could not read file %s", filename);
        return FLOCK_ERR_INVALID_ARGS;
    }
    fseek(file, 0, SEEK_END);
    length = ftell(file);
    fseek(file, 0, SEEK_SET);
    buffer = (char *)malloc(length + 1);
    if (buffer) {
        ssize_t r = fread(buffer, 1, length, file);
        (void)r;
        buffer[length] = '\0'; // Null-terminate the string
    } else {
        return FLOCK_ERR_ALLOCATION;
    }
    fclose(file);

    flock_return_t ret = flock_group_handle_create_from_serialized(
        client, buffer, length, mode, handle);
    free(buffer);
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
    flock_return_t ret = group_view_from_string(
        client->mid, serialized_view, view_size, &view, &credentials);
    if(ret != FLOCK_SUCCESS) return ret;

    flock_group_handle_t rh =
        (flock_group_handle_t)calloc(1, sizeof(*rh));

    if(!rh) {
        ret = FLOCK_ERR_ALLOCATION;
        goto finish;
    }

    const char* addr = view.members.data[0].address;

    hg_return_t hret = margo_addr_lookup(client->mid, addr, &(rh->addr));
    if(hret != HG_SUCCESS) {
        free(rh);
        flock_group_view_clear(&view);
        return FLOCK_ERR_FROM_MERCURY;
    }

    rh->client      = client;
    rh->provider_id = view.members.data[0].provider_id;
    rh->refcount    = 1;
    rh->view        = view;

    client->num_group_handles += 1;

    if(mode & FLOCK_MODE_INIT_UPDATE) {
        flock_return_t ret = flock_group_update_view(rh, NULL);
        if(ret != FLOCK_SUCCESS) {
            flock_group_handle_release(rh);
            goto finish;
        }
    }

    *handle = rh;

finish:
    return ret;
}

flock_return_t flock_group_serialize(
        flock_group_handle_t handle,
        void (*serializer)(void*, const char*, size_t),
        void* context)
{
    FLOCK_GROUP_VIEW_LOCK(&handle->view);
    flock_return_t ret =  group_view_serialize(
            handle->client->mid,
            handle->credentials,
            &handle->view,
            serializer,
            context);
    FLOCK_GROUP_VIEW_UNLOCK(&handle->view);
    return ret;
}

flock_return_t flock_group_serialize_to_file(
        flock_group_handle_t handle,
        const char* filename)
{
    FLOCK_GROUP_VIEW_LOCK(&handle->view);
    flock_return_t ret =  group_view_serialize_to_file(
            handle->client->mid,
            handle->credentials,
            &handle->view,
            filename);
    FLOCK_GROUP_VIEW_UNLOCK(&handle->view);
    return ret;
}

flock_return_t flock_group_size(
        flock_group_handle_t handle,
        size_t* size)
{
    // IMPORTANT: in the view.members structure, the "size" field
    // is the number of entries in the array, not the size of the
    // group. The size of the group is defined as R+1 where R is
    // the maximum rank found in the group.
    FLOCK_GROUP_VIEW_UNLOCK(&handle->view);
    if(handle->view.members.size == 0) {
        *size = 0;
    } else {
        flock_member_t* last_member = &handle->view.members.data[handle->view.members.size-1];
        *size = last_member->rank + 1;
    }
    FLOCK_GROUP_VIEW_UNLOCK(&handle->view);
    return FLOCK_SUCCESS;
}

flock_return_t flock_group_live_member_count(
        flock_group_handle_t handle,
        size_t* count)
{
    FLOCK_GROUP_VIEW_LOCK(&handle->view);
    *count = handle->view.members.size;
    FLOCK_GROUP_VIEW_UNLOCK(&handle->view);
    return FLOCK_SUCCESS;
}

flock_return_t flock_group_digest(
        flock_group_handle_t handle,
        uint64_t* digest)
{
    FLOCK_GROUP_VIEW_LOCK(&handle->view);
    *digest = handle->view.digest;
    FLOCK_GROUP_VIEW_UNLOCK(&handle->view);
    return FLOCK_SUCCESS;
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
        view->members.data[i].rank        = handle->view.members.data[i].rank;
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

flock_return_t flock_group_member_iterate(
          flock_group_handle_t handle,
          flock_member_access_fn access_fn,
          void* context)
{
    if(!access_fn) return FLOCK_SUCCESS;
    FLOCK_GROUP_VIEW_LOCK(&handle->view);
    // Call the callback on all members
    for(size_t i=0; i < handle->view.members.size; ++i) {
        if(!access_fn(context,
                handle->view.members.data[i].rank,
                handle->view.members.data[i].address,
                handle->view.members.data[i].provider_id))
            break;
    }
    FLOCK_GROUP_VIEW_UNLOCK(&handle->view);
    return FLOCK_SUCCESS;
}

flock_return_t flock_group_member_get_address_string(
          flock_group_handle_t handle,
          size_t rank,
          char** address)
{
    FLOCK_GROUP_VIEW_LOCK(&handle->view);
    const flock_member_t* member = flock_group_view_find_member(&handle->view, rank);
    if(!member) {
        FLOCK_GROUP_VIEW_UNLOCK(&handle->view);
        *address = NULL;
        return FLOCK_ERR_NO_MEMBER;
    }
    *address = strdup(member->address);
    FLOCK_GROUP_VIEW_UNLOCK(&handle->view);
    return FLOCK_SUCCESS;
}

flock_return_t flock_group_member_get_address(
          flock_group_handle_t handle,
          size_t rank,
          hg_addr_t* address)
{
    FLOCK_GROUP_VIEW_LOCK(&handle->view);
    const flock_member_t* member = flock_group_view_find_member(&handle->view, rank);
    if(!member) {
        FLOCK_GROUP_VIEW_UNLOCK(&handle->view);
        *address = HG_ADDR_NULL;
        return FLOCK_ERR_NO_MEMBER;
    }
    hg_return_t hret = margo_addr_lookup(
        handle->client->mid,
        member->address, address);
    FLOCK_GROUP_VIEW_UNLOCK(&handle->view);
    if(hret != HG_SUCCESS) return FLOCK_ERR_FROM_MERCURY;
    return FLOCK_SUCCESS;
}

flock_return_t flock_group_member_get_provider_id(
          flock_group_handle_t handle,
          size_t rank,
          uint16_t* provider_id)
{

    FLOCK_GROUP_VIEW_LOCK(&handle->view);
    const flock_member_t* member = flock_group_view_find_member(&handle->view, rank);
    if(!member) {
        FLOCK_GROUP_VIEW_UNLOCK(&handle->view);
        *provider_id = MARGO_MAX_PROVIDER_ID;
        return FLOCK_ERR_NO_MEMBER;
    }
    *provider_id = member->provider_id;
    FLOCK_GROUP_VIEW_UNLOCK(&handle->view);
    return FLOCK_SUCCESS;
}

flock_return_t flock_group_member_get_rank(
          flock_group_handle_t handle,
          const char* address,
          uint16_t provider_id,
          size_t* rank)
{
    *rank = SIZE_MAX;
    flock_return_t ret = FLOCK_ERR_NO_MEMBER;
    FLOCK_GROUP_VIEW_LOCK(&handle->view);
    for(size_t i=0; i < handle->view.members.size; ++i) {
        if(provider_id != handle->view.members.data[i].provider_id) continue;
        if(strcmp(address, handle->view.members.data[i].address) != 0) continue;
        *rank = handle->view.members.data[i].rank;
        ret = FLOCK_SUCCESS;
        break;
    }
    FLOCK_GROUP_VIEW_UNLOCK(&handle->view);
    return ret;
}

flock_return_t flock_group_metadata_iterate(
          flock_group_handle_t handle,
          flock_metadata_access_fn access_fn,
          void* context)
{
    if(!access_fn) return FLOCK_SUCCESS;
    FLOCK_GROUP_VIEW_LOCK(&handle->view);
    for(size_t i = 0; i < handle->view.metadata.size; ++i) {
        if(!access_fn(context,
                      handle->view.metadata.data[i].key,
                      handle->view.metadata.data[i].value))
            break;
    }
    FLOCK_GROUP_VIEW_UNLOCK(&handle->view);
    return FLOCK_SUCCESS;
}

flock_return_t flock_group_metadata_access(
          flock_group_handle_t handle,
          const char* key,
          flock_metadata_access_fn access_fn,
          void* context)
{
    flock_return_t ret = FLOCK_SUCCESS;
    if(!access_fn) return FLOCK_SUCCESS;
    FLOCK_GROUP_VIEW_UNLOCK(&handle->view);
    const char* value = flock_group_view_find_metadata(&handle->view, key);
    if(value) access_fn(context, key, value);
    else ret = FLOCK_ERR_NO_METADATA;
    FLOCK_GROUP_VIEW_UNLOCK(&handle->view);
    return ret;
}

flock_return_t flock_group_metadata_set(
          flock_group_handle_t handle,
          const char* key,
          const char* value)
{
    // TODO
}
