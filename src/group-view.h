/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef FLOCK_GROUP_VIEW_H
#define FLOCK_GROUP_VIEW_H

#include <mercury.h>
#include <mercury_macros.h>
#include <mercury_proc.h>
#include <mercury_proc_string.h>
#include <margo.h>
#include "flock/flock-group.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "uthash.h"

typedef struct {
    uint64_t  rank;
    uint16_t  provider_id;
    char*     str_addr;
} member_t;

typedef struct {
    char* key;
    char* value;
    UT_hash_handle hh;
} metadata_t;

typedef struct {
    size_t           size;
    size_t           capacity;
    member_t*        members;
    metadata_t*      metadata;
    ABT_mutex_memory mtx;
} group_view_t;

#define GROUP_VIEW_INITIALIZER {0,0,NULL,NULL,ABT_MUTEX_INITIALIZER}

#define LOCK_GROUP_VIEW(view) do {                             \
    ABT_mutex_lock(ABT_MUTEX_MEMORY_GET_HANDLE(&(view)->mtx)); \
} while(0)

#define UNLOCK_GROUP_VIEW(view) do {                             \
    ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&(view)->mtx)); \
} while(0)

static inline void clear_group_view(group_view_t *view)
{
    if (view) {
        LOCK_GROUP_VIEW(view);
        for (size_t i = 0; i < view->size; ++i) {
            free(view->members[i].str_addr);
        }
        free(view->members);
        view->capacity = 0;
        view->size     = 0;
        UNLOCK_GROUP_VIEW(view);
    }
}

static inline ssize_t group_view_binary_search(const group_view_t *view, uint64_t key)
{
    ssize_t left = 0;
    ssize_t right = view->size - 1;
    while (left <= right) {
        ssize_t mid = left + (right - left) / 2;
        if (view->members[mid].rank == key) {
            return mid; // Key found
        } else if (view->members[mid].rank < key) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }
    return -1; // Key not found
}

static inline bool group_view_add_member(
        group_view_t *view,
        uint64_t rank,
        uint16_t provider_id,
        const char *address)
{
    LOCK_GROUP_VIEW(view);
    bool ret = true;

    // Check if there is enough capacity, if not, resize
    if (view->size == view->capacity) {
        view->capacity *= 2;
        member_t *temp = (member_t *)realloc(view->members, view->capacity * sizeof(member_t));
        if (!temp) {
            UNLOCK_GROUP_VIEW(view);
            ret = false;
            goto finish;
        }
        view->members = temp;
    }

    // Find the position to insert while maintaining sorted order
    size_t pos = 0;
    while (pos < view->size && view->members[pos].rank < rank) {
        ++pos;
    }

    // Shift elements to make space for the new member
    memmove(&view->members[pos + 1], &view->members[pos], (view->size - pos) * sizeof(member_t));

    // Insert the new member
    view->members[pos].rank        = rank;
    view->members[pos].provider_id = provider_id;
    view->members[pos].str_addr    = strdup(address);
    if (!view->members[pos].str_addr) {
        UNLOCK_GROUP_VIEW(view);
        ret = false; // Memory allocation failed
        goto finish;
    }

    ++view->size;

finish:
    UNLOCK_GROUP_VIEW(view);
    return ret;
}

// Function to remove a member based on its rank
static inline bool group_view_remove_member(group_view_t *view, uint64_t rank)
{
    LOCK_GROUP_VIEW(view);
    bool ret = true;
    ssize_t idx = group_view_binary_search(view, rank);
    if (idx == -1) {
        ret = false;
        goto finish;
    }

    // Free the memory allocated for the address
    free(view->members[idx].str_addr);

    // Shift elements to remove the member
    memmove(&view->members[idx], &view->members[idx + 1], (view->size - idx - 1) * sizeof(member_t));

    --view->size;

finish:
    UNLOCK_GROUP_VIEW(view);
    return ret;
}

// Function to find a member based on its rank
static inline const member_t *group_view_find_member(const group_view_t *view, uint64_t rank)
{
    ssize_t idx = group_view_binary_search(view, rank);
    if (idx == -1) {
        return NULL; // member_t not found
    }
    return &view->members[idx];
}

// Function to get the value associated with a metadata key (returns NULL if not found)
static inline const char* group_view_find_metadata(const group_view_t *view, const char* key)
{
    metadata_t* md = NULL;
    HASH_FIND_STR(view->metadata, key, md);
    return md ? md->value : NULL;
}

// Function to iterate over the metadata
static inline void group_view_metadata_iterate(
        const group_view_t *view,
        flock_metadata_access_fn access_fn,
        void* context)
{
    metadata_t *md, *tmp;
    HASH_ITER(hh, view->metadata, md, tmp) {
        if(!access_fn(context, md->key, md->value)) goto finish;
    }
finish:
    return;
}

// Function to serialize/deserialize a group view
static inline hg_return_t hg_proc_group_view_t_internal(hg_proc_t proc, group_view_t* view) {
    hg_return_t ret = HG_SUCCESS;
    switch(hg_proc_get_op(proc)) {
    case HG_ENCODE:
        {
            ret = hg_proc_hg_size_t(proc, &view->size);
            if(ret != HG_SUCCESS) return ret;
            for(size_t i=0; i < view->size; ++i) {
                ret = hg_proc_uint64_t(proc, &view->members[i].rank);
                if(ret != HG_SUCCESS) return ret;
                ret = hg_proc_uint16_t(proc, &view->members[i].provider_id);
                if(ret != HG_SUCCESS) return ret;
                size_t addr_size = strlen(view->members[i].str_addr);
                ret = hg_proc_hg_size_t(proc, &addr_size);
                if(ret != HG_SUCCESS) return ret;
                ret = hg_proc_memcpy(proc, view->members[i].str_addr, addr_size);
                if(ret != HG_SUCCESS) return ret;
            }
            size_t count_metadata = HASH_COUNT(view->metadata);
            ret = hg_proc_hg_size_t(proc, &count_metadata);
            if(ret != HG_SUCCESS) return ret;
            metadata_t *current_md, *tmp;
            HASH_ITER(hh, view->metadata, current_md, tmp) {
                uint64_t ksize = strlen(current_md->key);
                uint64_t vsize = strlen(current_md->value);
                ret = hg_proc_uint64_t(proc, &ksize);
                if(ret != HG_SUCCESS) return ret;
                ret = hg_proc_memcpy(proc, current_md->key, ksize);
                if(ret != HG_SUCCESS) return ret;
                ret = hg_proc_uint64_t(proc, &vsize);
                if(ret != HG_SUCCESS) return ret;
                ret = hg_proc_memcpy(proc, current_md->value, vsize);
                if(ret != HG_SUCCESS) return ret;
            }
        }
        break;
    case HG_DECODE:
        {
            // Free anything already in the array and the array itself
            for(size_t i=0; i < view->size; ++i)
                free(view->members[i].str_addr);
            view->capacity = 0;
            free(view->members);
            // Deserialize members
            ret = hg_proc_hg_size_t(proc, &view->size);
            if(ret != HG_SUCCESS) return ret;
            view->capacity = view->size;
            view->members = (member_t*)malloc(view->size * sizeof(member_t));
            if(!view->members) return HG_NOMEM;
            for(size_t i=0; i < view->size; ++i) {
                ret = hg_proc_uint64_t(proc, &view->members[i].rank);
                if(ret != HG_SUCCESS) return ret;
                ret = hg_proc_uint16_t(proc, &view->members[i].provider_id);
                if(ret != HG_SUCCESS) return ret;
                size_t addr_size = 0;
                ret = hg_proc_hg_size_t(proc, &addr_size);
                if(ret != HG_SUCCESS) return ret;
                view->members[i].str_addr = calloc(addr_size + 1, 1);
                if(!view->members[i].str_addr) { // Allocation failed
                    view->size = i;
                    clear_group_view(view);
                    return HG_NOMEM;
                }
                ret = hg_proc_memcpy(proc, view->members[i].str_addr, addr_size);
                if(ret != HG_SUCCESS) { // Deserialization of address failed
                    view->size = i;
                    clear_group_view(view);
                    return ret;
                }
            }
            // Deserialize metadata
            size_t count_metadata = 0;
            ret = hg_proc_hg_size_t(proc, &count_metadata);
            if(ret != HG_SUCCESS) {
                clear_group_view(view);
                return ret;
            }
            for(size_t i = 0; i < count_metadata; ++i) {
                uint64_t ksize = 0, vsize = 0;
                metadata_t* md = (metadata_t*)calloc(1, sizeof(metadata_t));
                ret = hg_proc_uint64_t(proc, &ksize);
                if(ret != HG_SUCCESS) {
                    clear_group_view(view);
                    return ret;
                }
                ret = hg_proc_memcpy(proc, md->key, ksize);
                if(ret != HG_SUCCESS) {
                    clear_group_view(view);
                    return ret;
                }
                ret = hg_proc_uint64_t(proc, &vsize);
                if(ret != HG_SUCCESS) {
                    clear_group_view(view);
                    return ret;
                }
                ret = hg_proc_memcpy(proc, md->value, vsize);
                if(ret != HG_SUCCESS) {
                    clear_group_view(view);
                    return ret;
                }
                HASH_ADD_KEYPTR(hh, view->metadata, md->key, ksize, md);
            }
        }
        break;
    case HG_FREE:
        clear_group_view(view);
        break;
    }
    return HG_SUCCESS;
}

static inline hg_return_t hg_proc_group_view_t(hg_proc_t proc, group_view_t* view) {
    LOCK_GROUP_VIEW(view);
    hg_return_t hret = hg_proc_group_view_t_internal(proc, view);
    UNLOCK_GROUP_VIEW(view);
    return hret;
}

#endif /* FLOCK_GROUP_VIEW_H */

