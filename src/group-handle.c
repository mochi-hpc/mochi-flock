/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "types.h"
#include "client.h"
#include "group-view.h"
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

    if(mode & FLOCK_MODE_INIT_UPDATE) {
        char buffer[sizeof("flock")];
        size_t bufsize = sizeof("flock");
        hret = margo_provider_get_identity(client->mid, addr, provider_id, buffer, &bufsize);
        if(hret != HG_SUCCESS || strcmp("flock", buffer) != 0)
            return FLOCK_ERR_INVALID_PROVIDER;
    }

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
        flock_return_t ret = flock_group_update(rh, NULL);
        if(ret != FLOCK_SUCCESS) {
            flock_group_handle_release(rh);
            return ret;
        }
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

    hg_return_t hret;

    // Parse the content of the file
    struct json_tokener*    tokener = json_tokener_new();
    enum json_tokener_error jerr;
    struct json_object* content = json_tokener_parse_ex(tokener, serialized_view, view_size);
    if (!content) {
        jerr = json_tokener_get_error(tokener);
        margo_error(client->mid,
            "[flock] JSON parse error: %s",
            json_tokener_error_desc(jerr));
        json_tokener_free(tokener);
        return FLOCK_ERR_INVALID_CONFIG;
    }
    json_tokener_free(tokener);
    if (!(json_object_is_type(content, json_type_object))) {
        margo_error(client->mid, "[flock] Invalid JSON group description");
        json_object_put(content);
        return FLOCK_ERR_INVALID_CONFIG;
    }

    flock_return_t ret = FLOCK_SUCCESS;

    // Check that the content has the right format
    struct json_object* credentials = json_object_object_get(content, "credentials");
    struct json_object* transport   = json_object_object_get(content, "transport");
    struct json_object* members     = json_object_object_get(content, "members");
    struct json_object* metadata    = json_object_object_get(content, "metadata");

    if(credentials && !json_object_is_type(credentials, json_type_int)) {
        margo_error(client->mid, "[flock] \"credentials\" field should be an integer");
        ret = FLOCK_ERR_INVALID_CONFIG;
        goto finish;
    }

    if(!transport) {
        margo_error(client->mid, "[flock] \"transport\" field not found");
        ret = FLOCK_ERR_INVALID_CONFIG;
        goto finish;
    }

    if(!json_object_is_type(transport, json_type_string)) {
        margo_error(client->mid, "[flock] \"transport\" field should be of type string");
        ret = FLOCK_ERR_INVALID_CONFIG;
        goto finish;
    }

    if(metadata && !json_object_is_type(metadata, json_type_object)) {
        margo_error(client->mid, "[flock] \"metadata\" field should be of type object");
        ret = FLOCK_ERR_INVALID_CONFIG;
        goto finish;
    }

    if(!members) {
        margo_error(client->mid, "[flock] \"members\" field not found");
        ret = FLOCK_ERR_INVALID_CONFIG;
        goto finish;
    }

    if(!json_object_is_type(members, json_type_array)) {
        margo_error(client->mid, "[flock] \"members\" field should be of type array");
        ret = FLOCK_ERR_INVALID_CONFIG;
        goto finish;
    }

    if(json_object_array_length(members) == 0) {
        margo_error(client->mid, "[flock] \"members\" field should have at least one element");
        ret = FLOCK_ERR_INVALID_CONFIG;
        goto finish;
    }

    for(size_t i = 0; i < json_object_array_length(members); ++i) {
        struct json_object* member = json_object_array_get_idx(members, i);
        if(!json_object_is_type(member, json_type_object)) {
            margo_error(client->mid, "[flock] \"members[%llu]\" should be an object", i);
            ret = FLOCK_ERR_INVALID_CONFIG;
            goto finish;
        }
        struct json_object* address     = json_object_object_get(member, "address");
        struct json_object* rank        = json_object_object_get(member, "rank");
        struct json_object* provider_id = json_object_object_get(member, "provider_id");
        if(!address) {
            margo_error(client->mid, "[flock] \"members[%llu].address\" not found", i);
            ret = FLOCK_ERR_INVALID_CONFIG;
            goto finish;
        }
        if(!provider_id) {
            margo_error(client->mid, "[flock] \"members[%llu].provider_id\" not found", i);
            ret = FLOCK_ERR_INVALID_CONFIG;
            goto finish;
        }
        if(!rank) {
            margo_error(client->mid, "[flock] \"members[%llu].rank\" not found", i);
            ret = FLOCK_ERR_INVALID_CONFIG;
            goto finish;
        }
        if(!json_object_is_type(address, json_type_string)) {
            margo_error(client->mid, "[flock] \"members[%llu].address\" should be a string", i);
            ret = FLOCK_ERR_INVALID_CONFIG;
            goto finish;
        }
        if(!json_object_is_type(provider_id, json_type_int)) {
            margo_error(client->mid, "[flock] \"members[%llu].provider_id\" should be an integer", i);
            ret = FLOCK_ERR_INVALID_CONFIG;
            goto finish;
        }
        if(json_object_get_int64(provider_id) < 0 || json_object_get_int64(provider_id) > 65535) {
            margo_error(client->mid, "[flock] \"members[%llu].provider_id\" value out of allowed range", i);
            ret = FLOCK_ERR_INVALID_CONFIG;
            goto finish;
        }
        if(json_object_get_int64(provider_id) < 0) {
            margo_error(client->mid, "[flock] \"members[%llu].rank\" value cannot be negative", i);
            ret = FLOCK_ERR_INVALID_CONFIG;
            goto finish;
        }
    }

    // convert the JSON into the internal group view
    group_view_t view = GROUP_VIEW_INITIALIZER;
    for(size_t i = 0; i < json_object_array_length(members); ++i) {
        struct json_object* member      = json_object_array_get_idx(members, i);
        struct json_object* address     = json_object_object_get(member, "address");
        struct json_object* provider_id = json_object_object_get(member, "provider_id");
        struct json_object* rank        = json_object_object_get(member, "rank");
        group_view_add_member(&view,
            json_object_get_uint64(rank),
            (uint16_t)json_object_get_uint64(provider_id),
            json_object_get_string(address));
    }
    json_object_object_foreach(metadata, metadata_key, metadata_value) {
        metadata_t* md = (metadata_t*)calloc(1, sizeof(*md));
        md->key = strdup(metadata_key);
        md->value = strdup(json_object_get_string(metadata_value));
        HASH_ADD(hh, view.metadata, key, strlen(md->key), md);
    }

    flock_group_handle_t rh =
        (flock_group_handle_t)calloc(1, sizeof(*rh));

    if(!rh) {
        ret = FLOCK_ERR_ALLOCATION;
        goto finish;
    }

    const char* addr = view.members[0].str_addr;

    hret = margo_addr_lookup(client->mid, addr, &(rh->addr));
    if(hret != HG_SUCCESS) {
        free(rh);
        return FLOCK_ERR_FROM_MERCURY;
    }

    rh->client      = client;
    rh->provider_id = view.members[0].provider_id;
    rh->refcount    = 1;
    rh->view        = view;

    client->num_group_handles += 1;

    if(mode & FLOCK_MODE_INIT_UPDATE) {
        flock_return_t ret = flock_group_update(rh, NULL);
        if(ret != FLOCK_SUCCESS) {
            flock_group_handle_release(rh);
            goto finish;
        }
    }

    *handle = rh;

finish:
    json_object_put(content);
    return ret;
}

static inline bool metadata_to_json(void* ctx, const char* key, const char* value)
{
    struct json_object* map = (struct json_object*)ctx;
    json_object_object_add(map, key, json_object_new_string(value));
    return true;
}

flock_return_t flock_group_serialize(
        flock_group_handle_t handle,
        void (*serializer)(void*, const char*, size_t),
        void* context)
{
    struct json_object* view = json_object_new_object();
    if(!serializer) return FLOCK_ERR_INVALID_ARGS;

    LOCK_GROUP_VIEW(&handle->view);
    struct json_object* members = json_object_new_array_ext(handle->view.size);
    json_object_object_add(view, "members", members);
    for(size_t i=0; i < handle->view.size; ++i) {
        struct json_object* member = json_object_new_object();
        json_object_object_add(member,
            "address", json_object_new_string(handle->view.members[i].str_addr));
        json_object_object_add(member,
            "provider_id", json_object_new_uint64(handle->view.members[i].provider_id));
        json_object_array_add(members, member);
    }

    hg_class_t* hg_class = margo_get_class(handle->client->mid);
    json_object_object_add(
        view, "transport", json_object_new_string(HG_Class_get_protocol(hg_class)));

    json_object_object_add(
        view, "credentials", json_object_new_int64(handle->credentials));

    struct json_object* metadata = json_object_new_object();
    group_view_metadata_iterate(&handle->view, metadata_to_json, metadata);
    json_object_object_add(view, "metadata", metadata);

    size_t len;
    const char* str = json_object_to_json_string_length(view, 0, &len);
    if(!str) {
        UNLOCK_GROUP_VIEW(&handle->view);
        return FLOCK_ERR_ALLOCATION;
    }

    UNLOCK_GROUP_VIEW(&handle->view);
    serializer(context, str, len);

    json_object_put(view);
    return FLOCK_SUCCESS;
}

flock_return_t flock_group_size(
        flock_group_handle_t handle,
        size_t* size)
{
    // IMPORTANT: in the group_view structure, the "size" field
    // is the number of entries in the array, not the size of the
    // group. The size of the group is defined as R+1 where R is
    // the maximum rank found in the group.
    LOCK_GROUP_VIEW(&handle->view);
    member_t* last_member = handle->view.members + handle->view.size;
    *size = last_member->rank + 1;
    UNLOCK_GROUP_VIEW(&handle->view);
    return FLOCK_SUCCESS;
}

flock_return_t flock_group_live_member_count(
        flock_group_handle_t handle,
        size_t* count)
{
    LOCK_GROUP_VIEW(&handle->view);
    *count = handle->view.size;
    UNLOCK_GROUP_VIEW(&handle->view);
    return FLOCK_SUCCESS;
}

flock_return_t flock_group_member_iterate(
          flock_group_handle_t handle,
          flock_member_access_fn access_fn,
          void* context)
{
    if(!access_fn) return FLOCK_SUCCESS;
    LOCK_GROUP_VIEW(&handle->view);
    // Call the callback on all members
    for(size_t i=0; i < handle->view.size; ++i) {
        if(!access_fn(context,
                handle->view.members[i].rank,
                handle->view.members[i].str_addr,
                handle->view.members[i].provider_id))
            break;
    }
    UNLOCK_GROUP_VIEW(&handle->view);
    return FLOCK_SUCCESS;
}

flock_return_t flock_group_member_get_address_string(
          flock_group_handle_t handle,
          size_t rank,
          char** address)
{
    LOCK_GROUP_VIEW(&handle->view);
    const member_t* member = group_view_find_member(&handle->view, rank);
    if(!member) {
        UNLOCK_GROUP_VIEW(&handle->view);
        *address = NULL;
        return FLOCK_ERR_NO_MEMBER;
    }
    *address = strdup(member->str_addr);
    UNLOCK_GROUP_VIEW(&handle->view);
    return FLOCK_SUCCESS;
}

flock_return_t flock_group_member_get_address(
          flock_group_handle_t handle,
          size_t rank,
          hg_addr_t* address)
{
    LOCK_GROUP_VIEW(&handle->view);
    const member_t* member = group_view_find_member(&handle->view, rank);
    if(!member) {
        UNLOCK_GROUP_VIEW(&handle->view);
        *address = HG_ADDR_NULL;
        return FLOCK_ERR_NO_MEMBER;
    }
    hg_return_t hret = margo_addr_lookup(
        handle->client->mid,
        member->str_addr, address);
    UNLOCK_GROUP_VIEW(&handle->view);
    if(hret != HG_SUCCESS) return FLOCK_ERR_FROM_MERCURY;
    return FLOCK_SUCCESS;
}

flock_return_t flock_group_member_get_provider_id(
          flock_group_handle_t handle,
          size_t rank,
          uint16_t* provider_id)
{

    LOCK_GROUP_VIEW(&handle->view);
    const member_t* member = group_view_find_member(&handle->view, rank);
    if(!member) {
        UNLOCK_GROUP_VIEW(&handle->view);
        *provider_id = MARGO_MAX_PROVIDER_ID;
        return FLOCK_ERR_NO_MEMBER;
    }
    *provider_id = member->provider_id;
    UNLOCK_GROUP_VIEW(&handle->view);
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
    LOCK_GROUP_VIEW(&handle->view);
    for(size_t i=0; i < handle->view.size; ++i) {
        if(provider_id != handle->view.members[i].provider_id) continue;
        if(strcmp(address, handle->view.members[i].str_addr) != 0) continue;
        *rank = handle->view.members[i].rank;
        ret = FLOCK_SUCCESS;
        break;
    }
    UNLOCK_GROUP_VIEW(&handle->view);
    return ret;
}

flock_return_t flock_group_metadata_iterate(
          flock_group_handle_t handle,
          flock_metadata_access_fn access_fn,
          void* context)
{
    if(!access_fn) return FLOCK_SUCCESS;
    LOCK_GROUP_VIEW(&handle->view);
    group_view_metadata_iterate(&handle->view, access_fn, context);
    UNLOCK_GROUP_VIEW(&handle->view);
    return FLOCK_SUCCESS;
}

flock_return_t flock_group_metadata_access(
          flock_group_handle_t handle,
          const char* key,
          flock_metadata_access_fn access_fn,
          void* context)
{

    if(!access_fn) return FLOCK_SUCCESS;
    LOCK_GROUP_VIEW(&handle->view);
    const char* value = group_view_find_metadata(&handle->view, key);
    access_fn(context, key, value);
    UNLOCK_GROUP_VIEW(&handle->view);
    return FLOCK_SUCCESS;
}

flock_return_t flock_group_metadata_set(
          flock_group_handle_t handle,
          const char* key,
          const char* value)
{
    // TODO
}
