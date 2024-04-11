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
#include "flock/flock-common.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @file flock-group-view.h
 *
 * @brief This file provides the definition of the flock_flock_group_view_t structure
 * and can be used by backend implementers. It is not meant to be used by clients.
 */

/**
 * @brief Representation of a group member.
 */
typedef struct {
    uint64_t  rank;
    uint16_t  provider_id;
    char*     address;
    // The fields bellow can be used by backends to associate extra data
    // to each member in a group view. These fields are not serialized when
    // the view is transferred. The free function, if provided will be called
    // on the data pointer when a the member is removed from the view.
    struct {
        void* data;
        void (*free)(void*);
    } extra;
} flock_member_t;


/**
 * @brief Mercury serialization/deserialization function for flock_member_t.
 */
static inline hg_return_t hg_proc_flock_member_t(hg_proc_t proc, flock_member_t* member) {
    hg_return_t hret;
    if(hg_proc_get_op(proc) == HG_DECODE) {
        member->extra.data = NULL;
        member->extra.free = NULL;
    }
    hret = hg_proc_hg_uint64_t(proc, &member->rank);
    if(hret != HG_SUCCESS) return hret;
    hret = hg_proc_hg_uint16_t(proc, &member->provider_id);
    if(hret != HG_SUCCESS) return hret;
    hret = hg_proc_hg_string_t(proc, &member->address);
    if(hret != HG_SUCCESS) return hret;
    return HG_SUCCESS;
}

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
 *
 * A group view contains a dynamic array of members (flock_member_t),
 * a dynamic array of metadata (flock_metadata_t), a digest, and a
 * mutex to protect access to the view's fields.
 *
 * Important: while the fields can be read without the need for the
 * bellow flock_* functions and FLOCK_* macros, they SHOULD NOT BE
 * MODIFIED without calling these functions/macros. This is because
 * (1) these functions also keep the digest up-to-date when the view is
 * modified, and (2) these functions also ensure some invariants on
 * the content of the view, such as the fact that members are sorted
 * by rank, ranks are unique, metadata are sorted by key, keys are unique,
 * and so on. Any direct modification of these fields risk breaking these
 * invariants.
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
    // Digest of the group's content
    uint64_t digest;
    // Mutex to protect access to the group view
    ABT_mutex_memory mtx;
} flock_group_view_t;

#define FLOCK_GROUP_VIEW_INITIALIZER {{0,0,NULL},{0,0,NULL},0,ABT_MUTEX_INITIALIZER}

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
    (__dst__)->digest = (__src__)->digest;                                           \
    memcpy(&(__dst__)->members, &(__src__)->members, sizeof((__src__)->members));    \
    memcpy(&(__dst__)->metadata, &(__src__)->metadata, sizeof((__src__)->metadata)); \
    memset(&(__src__)->members, 0, sizeof((__src__)->members));                      \
    memset(&(__src__)->metadata, 0, sizeof((__src__)->metadata));                    \
    (__src__)->digest = 0;                                                           \
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
        flock_member_t* member = &view->members.data[i];
        free(member->address);
        if(member->extra.free)
            (member->extra.free)(member->extra.data);
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

    view->digest = 0;
}

/**
 * @brief Clear the extra fields of a flock_flock_group_view_t.
 *
 * @param view Group view to clear.
 */
static inline void flock_group_view_clear_extra(flock_group_view_t *view)
{
    for (size_t i = 0; i < view->members.size; ++i) {
        flock_member_t* member = &view->members.data[i];
        if(member->extra.free)
            (member->extra.free)(member->extra.data);
        member->extra.free = NULL;
        member->extra.data = NULL;
    }
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
 * @brief This function is used to compute string hashes to
 * update a group view's digest.
 *
 * @param str string to hash.
 *
 * @return a uint64_t hash.
 */
static inline uint64_t flock_djb2_hash(const char *str)
{
    uint64_t hash = 5381;
    int c;

    while((c = *str++))
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

    return hash;
}

/**
 * @brief This function is used to compute hashes to
 * update a group view's digest when a member is added
 * or removed.
 *
 * @param rank Member rank.
 * @param provider_id Member provider ID.
 * @param address Member address.
 *
 * @return a uint64_t hash.
 */
static inline uint64_t flock_hash_member(
    uint64_t rank,
    uint16_t provider_id,
    const char *address)
{
    uint64_t hash = flock_djb2_hash(address);
    for(unsigned i=0; i < sizeof(rank); ++i)
        hash = ((hash << 5) + hash) + ((char*)&rank)[i];
    for(unsigned i=0; i < sizeof(provider_id); ++i)
        hash = ((hash << 5) + hash) + ((char*)&provider_id)[i];
    return hash;
}

/**
 * @brief This function is used to compute metadata hashes to
 * update a group view's digest when a metadata is updated.
 *
 * @param key Key of the metadata to hash.
 * @param val Value of the metadata to hash.
 *
 * @return a uint64_t hash.
 */
static inline uint64_t flock_hash_metadata(const char *key, const char *val)
{
    uint64_t kh = flock_djb2_hash(key);
    uint64_t vh = flock_djb2_hash(val);
    // To avoid the (key,value) pair to be equivalent to the (value,key) pair,
    // we rotate the value's hash by 3 bytes
    vh = (vh << 3) | (vh >> ((sizeof(vh) * CHAR_BIT - 3) % (sizeof(vh) * CHAR_BIT)));
    return kh ^ vh;
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
 * @return a pointer to the added flock_member_t* if successful, NULL in case of allocation error.
 */
static inline bool flock_group_view_add_member(
        flock_group_view_t *view,
        uint64_t rank,
        uint16_t provider_id,
        const char *address)
{
    // Compute the new member's hash
    uint64_t member_hash = flock_hash_member(rank, provider_id, address);

    // Check if there is enough capacity, if not, resize
    if (view->members.size == view->members.capacity) {
        if (view->members.capacity == 0)
            view->members.capacity = 1;
        else
            view->members.capacity *= 2;
        flock_member_t *temp = (flock_member_t *)realloc(
            view->members.data, view->members.capacity * sizeof(flock_member_t));
        if (!temp) return NULL;
        view->members.data = temp;
    }

    // Find the position to insert while maintaining sorted order
    size_t pos = 0;
    while (pos < view->members.size && view->members.data[pos].rank < rank) {
        ++pos;
    }

    // Make copy of address
    char* tmp_address = strdup(address);
    if (!tmp_address) return NULL;

    // Shift elements to make space for the new member
    memmove(&view->members.data[pos+1], &view->members.data[pos],
            (view->members.size - pos) * sizeof(flock_member_t));

    // Insert the new member
    view->members.data[pos].rank        = rank;
    view->members.data[pos].provider_id = provider_id;
    view->members.data[pos].address     = tmp_address;
    view->members.data[pos].extra.data  = NULL;
    view->members.data[pos].extra.free  = NULL;

    ++view->members.size;

    // Update digest
    view->digest ^= member_hash;

    return &view->members.data[pos];
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

    flock_member_t* member = &view->members.data[idx];

    // Compute the hash of the member to remove
    uint64_t member_hash = flock_hash_member(
            member->rank,
            member->provider_id,
            member->address);

    // Free the memory allocated for the address
    free(member->address);

    // Free extra context attached by the backend
    if(member->extra.free) (member->extra.free)(member->extra.data);

    // Shift elements to remove the member
    memmove(&view->members.data[idx], &view->members.data[idx + 1],
            (view->members.size - idx - 1) * sizeof(flock_member_t));

    --view->members.size;

    // Update digest
    view->digest ^= member_hash;

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
    // Compute the hash of the key and value
    uint64_t metadata_hash = flock_hash_metadata(key, value);

    // Try to find existing entry
    ssize_t idx = flock_group_view_metadata_binary_search(view, key);
    if (idx != -1) {
        // Compute old metadata hash
        uint64_t old_metadata_hash = flock_hash_metadata(key, view->metadata.data[idx].value);
        // Free old value
        free(view->metadata.data[idx].value);
        view->metadata.data[idx].value = strdup(value);
        // Update the digest
        view->digest ^= old_metadata_hash ^ metadata_hash;
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

    // Update the digest
    view->digest ^= metadata_hash;

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

    // Compute the hash of the key and value
    uint64_t metadata_hash = flock_hash_metadata(
        view->metadata.data[idx].key, view->metadata.data[idx].value);

    // Free the memory allocated for the key and value
    free(view->metadata.data[idx].key);
    free(view->metadata.data[idx].value);

    // Shift elements to remove the member
    memmove(&view->metadata.data[idx], &view->metadata.data[idx + 1],
            (view->metadata.size - idx - 1) * sizeof(flock_metadata_t));

    --view->metadata.size;

    // Update the digest
    view->digest ^= metadata_hash;

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
static inline hg_return_t hg_proc_flock_group_view_t(hg_proc_t proc, void* args) {
    flock_group_view_t* view = (flock_group_view_t*)args;
    if(hg_proc_get_op(proc) == HG_DECODE)
        flock_group_view_clear(view);
    hg_return_t ret = HG_SUCCESS;
    ret = hg_proc_uint64_t(proc, &view->digest);
    if(ret != HG_SUCCESS) return ret;
    ret = hg_proc_hg_size_t(proc, &view->members.size);
    if(ret != HG_SUCCESS) return ret;
    ret = hg_proc_hg_size_t(proc, &view->metadata.size);
    if(ret != HG_SUCCESS) return ret;
    if(hg_proc_get_op(proc) == HG_DECODE) {
        view->members.data = (flock_member_t*)calloc(
            view->members.size, sizeof(view->members.data[0]));
        if(!view->members.data) {
            return HG_NOMEM;
        }
        view->metadata.data = (flock_metadata_t*)calloc(
            view->metadata.size, sizeof(view->metadata.data[0]));
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
static inline hg_return_t hg_proc_flock_protected_group_view_t(hg_proc_t proc, void* args) {
    if(!args) return HG_SUCCESS;
    flock_group_view_t* view = (flock_group_view_t*)args;
    FLOCK_GROUP_VIEW_LOCK(view);
    hg_return_t hret = hg_proc_flock_group_view_t(proc, view);
    FLOCK_GROUP_VIEW_UNLOCK(view);
    return hret;
}

#ifdef __cplusplus
}
#endif

#endif /* FLOCK_GROUP_VIEW_H */

