/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
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

    /* Write to a unique temporary file, then atomically rename it onto the
     * target. Using mkstemp (rather than a fixed "<name>.swp") means concurrent
     * serializations -- from multiple threads, or even multiple processes writing
     * the same group file -- never share a temporary file, so a reader always
     * observes a complete, self-consistent file after the rename. */
    size_t l = strlen(data->filename);
    char* tmpname = calloc(l + 8, 1); /* room for ".XXXXXX" + '\0' */
    if(!tmpname) {
        // LCOV_EXCL_START
        data->ret = FLOCK_ERR_ALLOCATION;
        return;
        // LCOV_EXCL_STOP
    }
    sprintf(tmpname, "%s.XXXXXX", data->filename);

    int fd = mkstemp(tmpname);
    if(fd < 0) {
        // LCOV_EXCL_START
        margo_error(NULL, "[flock] Could not create temporary file for %s: %s",
                    data->filename, strerror(errno));
        data->ret = FLOCK_ERR_OTHER;
        free(tmpname);
        return;
        // LCOV_EXCL_STOP
    }
    fchmod(fd, 0644); /* mkstemp creates the file 0600; match fopen("w")'s mode */

    data->ret = FLOCK_ERR_OTHER; /* pessimistic until the rename succeeds */
    FILE* file = fdopen(fd, "w");
    if(!file) {
        // LCOV_EXCL_START
        margo_error(NULL, "[flock] Could not open %s: %s", tmpname, strerror(errno));
        close(fd);
        goto finish;
        // LCOV_EXCL_STOP
    }
    size_t written = fwrite(content, 1, size, file);
    int close_rc = fclose(file); /* fclose also closes fd */
    if(written != size || close_rc != 0) {
        // LCOV_EXCL_START
        margo_error(NULL, "[flock] Could not write file %s: %s", tmpname, strerror(errno));
        goto finish;
        // LCOV_EXCL_STOP
    }
    if(rename(tmpname, data->filename) != 0) {
        // LCOV_EXCL_START
        margo_error(NULL, "[flock] Could not rename file %s into %s: %s",
                    tmpname, data->filename, strerror(errno));
        goto finish;
        // LCOV_EXCL_STOP
    }
    data->ret = FLOCK_SUCCESS;
finish:
    if(data->ret != FLOCK_SUCCESS) unlink(tmpname); /* leave no stray temp file */
    free(tmpname);
}

flock_return_t flock_group_view_serialize(
        const flock_group_view_t* v,
        void (*serializer)(void*, const char*, size_t),
        void* context)
{
    // LCOV_EXCL_START
    if(!serializer) return FLOCK_ERR_INVALID_ARGS;
    // LCOV_EXCL_STOP

    struct json_object* view     = json_object_new_object();
    struct json_object* members  = json_object_new_array();
    json_object_object_add(view, "members", members);
    struct json_object* metadata = json_object_new_object();
    json_object_object_add(view, "metadata", metadata);

    /* Read the view under its lock: a concurrent add/remove can realloc or free
     * the members array, so it must not be observed mid-update. json_object_new_string
     * copies each string, so once the JSON is built it is a self-contained
     * snapshot and the lock can be dropped before the (potentially slow)
     * stringification and serializer callback (which does file I/O). */
    FLOCK_GROUP_VIEW_LOCK((flock_group_view_t*)v);
    for(size_t i=0; i < v->members.size; ++i) {
        struct json_object* member = json_object_new_object();
        json_object_object_add(member,
                "address", json_object_new_string(v->members.data[i].address));
        json_object_object_add(member,
                "provider_id", json_object_new_int64((int64_t)v->members.data[i].provider_id));
        json_object_array_add(members, member);
    }
    for(size_t i=0; i < v->metadata.size; ++i) {
        json_object_object_add(
                metadata, v->metadata.data[i].key,
                json_object_new_string(v->metadata.data[i].value));
    }
    FLOCK_GROUP_VIEW_UNLOCK((flock_group_view_t*)v);

    size_t len;
    const char* str = json_object_to_json_string_length(view, JSON_C_TO_STRING_NOSLASHESCAPE, &len);
    // LCOV_EXCL_START
    if(!str) {
        json_object_put(view);
        return FLOCK_ERR_ALLOCATION;
    }
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
    FILE* file = fopen(filename, "r");
    if(!file) {
        margo_error(NULL, "[flock] Could not read file %s", filename);
        return FLOCK_ERR_INVALID_ARGS;
    }

    if(fseek(file, 0, SEEK_END) != 0) {
        // LCOV_EXCL_START
        margo_error(NULL, "[flock] Could not seek in file %s", filename);
        fclose(file);
        return FLOCK_ERR_OTHER;
        // LCOV_EXCL_STOP
    }
    long length = ftell(file);
    if(length < 0) {
        // LCOV_EXCL_START
        margo_error(NULL, "[flock] Could not get size of file %s: %s",
                    filename, strerror(errno));
        fclose(file);
        return FLOCK_ERR_OTHER;
        // LCOV_EXCL_STOP
    }
    rewind(file);

    char* buffer = (char*)malloc((size_t)length + 1);
    if(!buffer) {
        // LCOV_EXCL_START
        fclose(file);
        return FLOCK_ERR_ALLOCATION;
        // LCOV_EXCL_STOP
    }

    size_t r = fread(buffer, 1, (size_t)length, file);
    int read_error = ferror(file);
    fclose(file);

    if(read_error) {
        // LCOV_EXCL_START
        margo_error(NULL, "[flock] Could not read file %s", filename);
        free(buffer);
        return FLOCK_ERR_OTHER;
        // LCOV_EXCL_STOP
    }
    if(r == 0) {
        margo_error(NULL, "[flock] Group file %s is empty", filename);
        free(buffer);
        return FLOCK_ERR_INVALID_CONFIG;
    }
    buffer[r] = '\0'; // Null-terminate at the number of bytes actually read

    // Parse exactly the bytes read, never the ftell length: a short read (for
    // instance the file changing under us) must not hand uninitialized bytes to
    // the parser.
    flock_return_t ret = flock_group_view_from_string(buffer, r, view);
    free(buffer);
    return ret;
}
