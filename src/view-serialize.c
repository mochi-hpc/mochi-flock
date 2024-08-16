/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <json-c/json.h>
#include "flock/flock-common.h"
#include "flock/flock-group-view.h"

struct file_serializer_data {
    const char*    filename;
    flock_return_t ret;
};

static inline void file_serializer(void* uargs, const char* content, size_t size)
{
    struct file_serializer_data* data = (struct file_serializer_data*)uargs;
    size_t l = strlen(data->filename);
    char* filename = calloc(l+5, 1);
    sprintf(filename, "%s.swp", data->filename);
    FILE* file = fopen(filename, "w");
    if(!file) {
        // LCOV_EXCL_START
        margo_error(NULL, "[flock] Could not open %s: %s", filename, strerror(errno));
        data->ret = FLOCK_ERR_ALLOCATION;
        goto finish;
        // LCOV_EXCL_STOP
    }
    size_t written = fwrite(content, 1, size, file);
    if(written != size) {
        // LCOV_EXCL_START
        margo_error(NULL, "[flock] Could not write file %s: %s", filename, strerror(errno));
        data->ret = FLOCK_ERR_OTHER;
        goto finish;
        // LCOV_EXCL_STOP
    }
    fclose(file);
    file = NULL;
    if(rename(filename, data->filename) != 0) {
        // LCOV_EXCL_START
        margo_error(NULL, "[flock] Could not rename file %s into %s: %s",
                    filename, data->filename, strerror(errno));
        data->ret = FLOCK_ERR_OTHER;
        goto finish;
        // LCOV_EXCL_STOP
    }
    data->ret = FLOCK_SUCCESS;
finish:
    if(file) fclose(file);
    free(filename);
}

flock_return_t flock_group_view_serialize(
        const flock_group_view_t* v,
        void (*serializer)(void*, const char*, size_t),
        void* context)
{
    struct json_object* view = json_object_new_object();
    // LCOV_EXCL_START
    if(!serializer) return FLOCK_ERR_INVALID_ARGS;
    // LCOV_EXCL_STOP

    struct json_object* members = json_object_new_array();
    json_object_object_add(view, "members", members);
    for(size_t i=0; i < v->members.size; ++i) {
        struct json_object* member = json_object_new_object();
        json_object_object_add(member,
                "address", json_object_new_string(v->members.data[i].address));
        json_object_object_add(member,
                "provider_id", json_object_new_int64((int64_t)v->members.data[i].provider_id));
        json_object_array_add(members, member);
    }

    struct json_object* metadata = json_object_new_object();
    for(size_t i=0; i < v->metadata.size; ++i) {
        json_object_object_add(
                metadata, v->metadata.data[i].key,
                json_object_new_string(v->metadata.data[i].value));
    }
    json_object_object_add(view, "metadata", metadata);

    size_t len;
    const char* str = json_object_to_json_string_length(view, JSON_C_TO_STRING_NOSLASHESCAPE, &len);
    // LCOV_EXCL_START
    if(!str) return FLOCK_ERR_ALLOCATION;
    // LCOV_EXCL_STOP

    serializer(context, str, len);

    json_object_put(view);
    return FLOCK_SUCCESS;
}

flock_return_t flock_group_view_serialize_to_file(
        const flock_group_view_t* v,
        const char* filename)
{
    struct file_serializer_data context = {
        .filename = filename,
        .ret = FLOCK_SUCCESS
    };
    flock_return_t ret = flock_group_view_serialize(v, file_serializer, &context);
    if(ret != FLOCK_SUCCESS) return ret;
    else return context.ret;
}

flock_return_t flock_group_view_from_string(
        const char* str,
        size_t str_len,
        flock_group_view_t* view)
{
    // Parse the content of the file
    struct json_tokener*    tokener = json_tokener_new();
    enum json_tokener_error jerr;
    struct json_object* content = json_tokener_parse_ex(tokener, str, str_len);
    if (!content) {
        jerr = json_tokener_get_error(tokener);
        margo_error(NULL, "[flock] JSON parse error: %s",
                json_tokener_error_desc(jerr));
        json_tokener_free(tokener);
        return FLOCK_ERR_INVALID_CONFIG;
    }
    json_tokener_free(tokener);
    if (!(json_object_is_type(content, json_type_object))) {
        margo_error(NULL, "[flock] Invalid JSON group description");
        json_object_put(content);
        return FLOCK_ERR_INVALID_CONFIG;
    }

    flock_return_t ret = FLOCK_SUCCESS;

    // Check that the content has the right format
    struct json_object* members   = json_object_object_get(content, "members");
    struct json_object* metadata  = json_object_object_get(content, "metadata");

    if(metadata && !json_object_is_type(metadata, json_type_object)) {
        margo_error(NULL, "[flock] \"metadata\" field should be of type object");
        ret = FLOCK_ERR_INVALID_CONFIG;
        goto finish;
    }

    if(!members) {
        margo_error(NULL, "[flock] \"members\" field not found");
        ret = FLOCK_ERR_INVALID_CONFIG;
        goto finish;
    }

    if(!json_object_is_type(members, json_type_array)) {
        margo_error(NULL, "[flock] \"members\" field should be of type array");
        ret = FLOCK_ERR_INVALID_CONFIG;
        goto finish;
    }

    if(json_object_array_length(members) == 0) {
        margo_error(NULL, "[flock] \"members\" field should have at least one element");
        ret = FLOCK_ERR_INVALID_CONFIG;
        goto finish;
    }

    for(size_t i = 0; i < json_object_array_length(members); ++i) {
        struct json_object* member = json_object_array_get_idx(members, i);
        if(!json_object_is_type(member, json_type_object)) {
            margo_error(NULL, "[flock] \"members[%llu]\" should be an object", i);
            ret = FLOCK_ERR_INVALID_CONFIG;
            goto finish;
        }
        struct json_object* address     = json_object_object_get(member, "address");
        struct json_object* provider_id = json_object_object_get(member, "provider_id");
        if(!address) {
            margo_error(NULL, "[flock] \"members[%llu].address\" not found", i);
            ret = FLOCK_ERR_INVALID_CONFIG;
            goto finish;
        }
        if(!provider_id) {
            margo_error(NULL, "[flock] \"members[%llu].provider_id\" not found", i);
            ret = FLOCK_ERR_INVALID_CONFIG;
            goto finish;
        }
        if(!json_object_is_type(address, json_type_string)) {
            margo_error(NULL, "[flock] \"members[%llu].address\" should be a string", i);
            ret = FLOCK_ERR_INVALID_CONFIG;
            goto finish;
        }
        if(!json_object_is_type(provider_id, json_type_int)) {
            margo_error(NULL, "[flock] \"members[%llu].provider_id\" should be an integer", i);
            ret = FLOCK_ERR_INVALID_CONFIG;
            goto finish;
        }
        if(json_object_get_int64(provider_id) < 0 || json_object_get_int64(provider_id) > 65535) {
            margo_error(NULL, "[flock] \"members[%llu].provider_id\" value out of allowed range", i);
            ret = FLOCK_ERR_INVALID_CONFIG;
            goto finish;
        }
        if(json_object_get_int64(provider_id) < 0) {
            margo_error(NULL, "[flock] \"members[%llu].rank\" value cannot be negative", i);
            ret = FLOCK_ERR_INVALID_CONFIG;
            goto finish;
        }
    }

    // convert the JSON into the internal group view
    flock_group_view_clear(view);
    for(size_t i = 0; i < json_object_array_length(members); ++i) {
        struct json_object* member      = json_object_array_get_idx(members, i);
        struct json_object* address     = json_object_object_get(member, "address");
        struct json_object* provider_id = json_object_object_get(member, "provider_id");
        flock_group_view_add_member(view,
                json_object_get_string(address),
                (uint16_t)json_object_get_int64(provider_id));
    }
    json_object_object_foreach(metadata, metadata_key, metadata_value) {
        flock_group_view_add_metadata(view, metadata_key,
                json_object_get_string(metadata_value));
    }

finish:
    json_object_put(content);
    return ret;
}

flock_return_t flock_group_view_from_file(
        const char* filename,
        flock_group_view_t* view)
{
    // Read the content of the file into a buffer
    char* buffer = NULL;
    size_t length;
    FILE* file = fopen(filename, "r");
    if(!file) {
        margo_error(NULL, "[flock] Could not read file %s", filename);
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
    flock_return_t ret = flock_group_view_from_string(buffer, length, view);
    free(buffer);
    return ret;
}
