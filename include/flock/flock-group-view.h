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

/**
 * @file flock-group-view.h
 *
 * @brief This file provides the definition of the flock_flock_group_view_t structure
 * and can be used by backend implementers. It is not meant to be used by clients.
 */

/**
 * @brief Representation of a group member.
 *
 * Will define the following structure:
 *
 * ```
 * typedef struct {
 *     uint64_t  rank;
 *     uint16_t  provider_id;
 *     char*     address;
 * } flock_member_t;
 * ```
 */
MERCURY_GEN_PROC(flock_member_t,
    ((uint64_t)(rank))\
    ((uint16_t)(provider_id))\
    ((hg_string_t)(address)))

/**
 * @brief Key/value pair.
 *
 * Will define the following structure:
 *
 * ```
 * typedef struct {
 *     char* key;
 *     char* value;
 * } flock_metadata_t;
 * ```
 */
MERCURY_GEN_PROC(flock_metadata_t,
    ((hg_string_t)(key))\
    ((hg_string_t)(value)))

/**
 * @brief Group view.
 */
typedef struct {
    // Dynamic array of members (sorted by rank)
    struct {
        uint64_t        size;
        uint64_t        capacity;
        flock_member_t* data;
    } members;
    // Dynamic array of key/value pairs (sorted by keys)
    struct {
        uint64_t          size;
        uint64_t          capacity;
        flock_metadata_t* data;
    } metadata;
    ABT_mutex_memory mtx;
} flock_group_view_t;

#define FLOCK_GROUP_VIEW_INITIALIZER {{0,0,NULL},{0,0,NULL},ABT_MUTEX_INITIALIZER}

/**
 * @brief This macro takes two pointers to flock_group_view_t and moves
 * the content of the source into the destination.
 *
 * @warning This macro assumes that the destination is empty. If it is not,
 * it may cause a member leak as the destination looses the pointers to its data.
 *
 * @param __src__ flock_group_view_t* from which to move.
 * @param __dst__ flock_group_view_t* into which to move.
 */
#define FLOCK_GROUP_VIEW_MOVE(__src__, __dst__) do {                                 \
    memcpy(&(__dst__)->members, &(__src__)->members, sizeof((__src__)->members));    \
    memcpy(&(__dst__)->metadata, &(__src__)->metadata, sizeof((__src__)->metadata)); \
    memset(&(__src__)->members, 0, sizeof((__src__)->members));                      \
    memset(&(__src__)->metadata, 0, sizeof((__src__)->metadata));                    \
} while(0)

#define FLOCK_GROUP_VIEW_LOCK(view) do {                       \
    ABT_mutex_lock(ABT_MUTEX_MEMORY_GET_HANDLE(&(view)->mtx)); \
} while(0)

#define FLOCK_GROUP_VIEW_UNLOCK(view) do {                       \
    ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&(view)->mtx)); \
} while(0)

/**
 * @brief Clear the content of a flock_flock_group_view_t.
 *
 * @param view Group view to clear.
 */
static inline void flock_group_view_clear(flock_group_view_t *view)
{
    for (size_t i = 0; i < view->members.size; ++i) {
        free(view->members.data[i].address);
    }
    free(view->members.data);
    view->members.data     = NULL;
    view->members.capacity = 0;
    view->members.size     = 0;

    for (size_t i = 0; i < view->metadata.size; ++i) {
        free(view->metadata.data[i].key);
        free(view->metadata.data[i].value);
    }
    free(view->metadata.data);
    view->metadata.data     = NULL;
    view->metadata.capacity = 0;
    view->metadata.size     = 0;
}

/**
 * @brief Binary search through the sorted array of members.
 *
 * @param view View to search in.
 * @param rank Rank to search for.
 *
 * @return The index of the member found, or -1 if not found.
 */
static inline ssize_t flock_group_view_members_binary_search(
    const flock_group_view_t *view, uint64_t rank)
{
    ssize_t left = 0;
    ssize_t right = view->members.size - 1;
    while (left <= right) {
        ssize_t mid = left + (right - left) / 2;
        if (view->members.data[mid].rank == rank) {
            return mid; // Rank found
        } else if (view->members.data[mid].rank < rank) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }
    return -1; // Rank not found
}

/**
 * @brief Add a member to the view.
 *
 * @important The caller is responsible for checking that they are
 * not adding a member with a rank that is already in use.
 *
 * @param view View in which to add the member.
 * @param rank Rank of the new member.
 * @param provider_id Provider ID of the new member.
 * @param address Address of the new member.
 *
 * @return true if added, false in case of allocation error.
 */
static inline bool flock_group_view_add_member(
        flock_group_view_t *view,
        uint64_t rank,
        uint16_t provider_id,
        const char *address)
{
    // Check if there is enough capacity, if not, resize
    if (view->members.size == view->members.capacity) {
        if (view->members.capacity == 0)
            view->members.capacity = 1;
        else
            view->members.capacity *= 2;
        flock_member_t *temp = (flock_member_t *)realloc(
            view->members.data, view->members.capacity * sizeof(flock_member_t));
        if (!temp) return false;
        view->members.data = temp;
    }

    // Find the position to insert while maintaining sorted order
    size_t pos = 0;
    while (pos < view->members.size && view->members.data[pos].rank < rank) {
        ++pos;
    }

    // Make copy of address
    char* tmp_address = strdup(address);
    if (!tmp_address) return false;

    // Shift elements to make space for the new member
    memmove(&view->members.data[pos+1], &view->members.data[pos],
            (view->members.size - pos) * sizeof(flock_member_t));

    // Insert the new member
    view->members.data[pos].rank        = rank;
    view->members.data[pos].provider_id = provider_id;
    view->members.data[pos].address     = tmp_address;

    ++view->members.size;

    return true;
}

/**
 * @brief Removes a member given its rank.
 *
 * @param view View from which to remove the member.
 * @param rank Rank of the member to remove.
 *
 * @return true if the member was removed, false if it wasn't found.
 */
static inline bool flock_group_view_remove_member(flock_group_view_t *view, uint64_t rank)
{
    ssize_t idx = flock_group_view_members_binary_search(view, rank);
    if (idx == -1) return false;

    // Free the memory allocated for the address
    free(view->members.data[idx].address);

    // Shift elements to remove the member
    memmove(&view->members.data[idx], &view->members.data[idx + 1],
            (view->members.size - idx - 1) * sizeof(flock_member_t));

    --view->members.size;

    return true;
}

/**
 * @brief Find a member from its rank.
 *
 * @param view View in which to search.
 * @param rank Rank of the member to find.
 *
 * @return A pointer to the member, or NULL if not found.
 */
static inline const flock_member_t *flock_group_view_find_member(const flock_group_view_t *view, uint64_t rank)
{
    ssize_t idx = flock_group_view_members_binary_search(view, rank);
    if (idx == -1) {
        return NULL; // Member not found
    }
    return &view->members.data[idx];
}

/**
 * @brief Binary search through the sorted array of metadata entries.
 *
 * @param view View to search in.
 * @param key Key to search for.
 *
 * @return The index of the metadata found, or -1 if not found.
 */
static inline ssize_t flock_group_view_metadata_binary_search(
    const flock_group_view_t *view, const char* key)
{
    ssize_t left = 0;
    ssize_t right = view->metadata.size - 1;
    while (left <= right) {
        ssize_t mid = left + (right - left) / 2;
        int cmp = strcmp(view->metadata.data[mid].key, key);
        if (cmp == 0) {
            return mid; // Key found
        } else if (cmp < 0) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }
    return -1; // Key not found
}

/**
 * @brief Add a metadata to the view. If a metadata with the same
 * key already exists, its value will be replaced.
 *
 * @param view View in which to add the metadata.
 * @param rank Rank of the new metadata.
 * @param provider_id Provider ID of the new metadata.
 * @param address Address of the new member.
 *
 * @return true if added, false in case of allocation error.
 */
static inline bool flock_group_view_add_metadata(
        flock_group_view_t *view,
        const char* key,
        const char* value)
{
    // Try to find existing entry
    ssize_t idx = flock_group_view_metadata_binary_search(view, key);
    if (idx != -1) {
        free(view->metadata.data[idx].value);
        view->metadata.data[idx].value = strdup(value);
        return true;
    }

    // Check if there is enough capacity, if not, resize
    if (view->metadata.size == view->metadata.capacity) {
        if (view->metadata.capacity == 0)
            view->metadata.capacity = 1;
        else
            view->metadata.capacity *= 2;
        flock_metadata_t *temp = (flock_metadata_t *)realloc(
            view->metadata.data, view->metadata.capacity * sizeof(flock_metadata_t));
        if (!temp) return false;
        view->metadata.data = temp;
    }

    // Find the position to insert while maintaining sorted order
    size_t pos = 0;
    while (pos < view->metadata.size && strcmp(view->metadata.data[pos].key, key) < 0) {
        ++pos;
    }

    // Allocate temporaries
    char* tmp_key = strdup(key);
    if(!tmp_key) return false;
    char* tmp_value = strdup(value);
    if(!tmp_value) {
        free(tmp_key);
        return false;
    }

    // Shift elements to make space for the new metadata
    memmove(&view->metadata.data[pos+1], &view->metadata.data[pos],
            (view->metadata.size - pos) * sizeof(flock_metadata_t));

    // Insert the new metadata
    view->metadata.data[pos].key   = tmp_key;
    view->metadata.data[pos].value = tmp_value;

    ++view->metadata.size;

    return true;
}

/**
 * @brief Removes a metadata given its key.
 *
 * @param view View from which to remove the metadata.
 * @param rank Key of the metadata to remove.
 *
 * @return true if the metadata was removed, false if it wasn't found.
 */
static inline bool flock_group_view_remove_metadata(flock_group_view_t *view, const char* key)
{
    ssize_t idx = flock_group_view_metadata_binary_search(view, key);
    if (idx == -1) return false;

    // Free the memory allocated for the key and value
    free(view->metadata.data[idx].key);
    free(view->metadata.data[idx].value);

    // Shift elements to remove the member
    memmove(&view->metadata.data[idx], &view->metadata.data[idx + 1],
            (view->metadata.size - idx - 1) * sizeof(flock_metadata_t));

    --view->metadata.size;

    return true;
}

/**
 * @brief Find a metadata from its key.
 *
 * @param view View in which to search.
 * @param key Key of the metadata to find.
 *
 * @return A pointer to the value, or NULL if not found.
 */
static inline const char *flock_group_view_find_metadata(const flock_group_view_t *view, const char* key)
{
    ssize_t idx = flock_group_view_metadata_binary_search(view, key);
    if (idx == -1) {
        return NULL; // Member not found
    }
    return view->metadata.data[idx].value;
}

/**
 * @brief Serialize/deserialize a group view.
 *
 * @param proc Mercury proc object.
 * @param view View to serialize/deserialize.
 *
 * @return hg_return_t code.
 */
static inline hg_return_t hg_proc_flock_group_view_t(hg_proc_t proc, flock_group_view_t* view) {
    hg_return_t ret = HG_SUCCESS;
    ret = hg_proc_hg_size_t(proc, &view->members.size);
    if(ret != HG_SUCCESS) return ret;
    ret = hg_proc_hg_size_t(proc, &view->metadata.size);
    if(ret != HG_SUCCESS) return ret;
    if(hg_proc_get_op(proc) == HG_DECODE) {
        view->members.data = (flock_member_t*)malloc(
            view->members.size * sizeof(view->members.data[0]));
        if(!view->members.data) {
            return HG_NOMEM;
        }
        view->metadata.data = (flock_metadata_t*)malloc(
            view->metadata.size * sizeof(view->metadata.data[0]));
        if(!view->metadata.data) {
            free(view->members.data);
            return HG_NOMEM;
        }
        view->members.capacity = view->members.size;
        view->metadata.capacity = view->metadata.size;
    }
    for(size_t i = 0; i < view->members.size; ++i) {
        ret = hg_proc_uint64_t(proc, &view->members.data[i].rank);
        if(ret != HG_SUCCESS) return ret;
        ret = hg_proc_hg_string_t(proc, &view->members.data[i].address);
        if(ret != HG_SUCCESS) return ret;
        ret = hg_proc_uint16_t(proc, &view->members.data[i].provider_id);
        if(ret != HG_SUCCESS) return ret;
    }
    for(size_t i = 0; i < view->metadata.size; ++i) {
        ret = hg_proc_hg_string_t(proc, &view->metadata.data[i].key);
        if(ret != HG_SUCCESS) return ret;
        ret = hg_proc_hg_string_t(proc, &view->metadata.data[i].value);
        if(ret != HG_SUCCESS) return ret;
    }
    if(hg_proc_get_op(proc) == HG_FREE) {
        free(view->members.data);
        free(view->metadata.data);
        view->members.size = view->members.capacity = 0;
        view->metadata.size = view->metadata.capacity = 0;
    }
    return HG_SUCCESS;
}

/**
 * @brief Serializes/deserializes a flock_group_view_t and protect its access with a lock.
 *
 * @param proc Mercury proc object.
 * @param view View to serialize/deserialize.
 *
 * @return hg_return_t code.
 */
static inline hg_return_t hg_proc_flock_protected_group_view_t(hg_proc_t proc, flock_group_view_t* view) {
    FLOCK_GROUP_VIEW_LOCK(view);
    hg_return_t hret = hg_proc_flock_group_view_t(proc, view);
    FLOCK_GROUP_VIEW_UNLOCK(view);
    return hret;
}

#endif /* FLOCK_GROUP_VIEW_H */

