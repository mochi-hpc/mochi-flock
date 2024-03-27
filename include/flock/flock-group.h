/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FLOCK_GROUP_H
#define __FLOCK_GROUP_H

#include <margo.h>
#include <flock/flock-common.h>
#include <flock/flock-client.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct flock_group_handle *flock_group_handle_t;
#define FLOCK_GROUP_HANDLE_NULL ((flock_group_handle_t)NULL)

/**
 * @brief Handle for a non-blocking request.
 */
typedef struct flock_request* flock_request_t;

/**
 * @brief Creates a FLOCK group handle by contacting the group member
 * specified by the given address and provider ID.
 *
 * @param[in] client FLOCK client responsible for the group handle
 * @param[in] addr Mercury address of the provider
 * @param[in] provider_id id of the provider
 * @param[in] mode Optional mode
 * @param[out] handle group handle
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_group_handle_create(
        flock_client_t client,
        hg_addr_t addr,
        uint16_t provider_id,
        uint32_t mode,
        flock_group_handle_t* handle);

/**
 * @brief Creates a FLOCK group handle by reading the specified file.
 *
 * @param[in] client FLOCK client responsible for the group handle
 * @param[in] filename File name of the group file
 * @param[in] mode Optional mode
 * @param[out] handle group handle
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 *
 * Note: a FLOCK group file is a JSON file with the following format.
 * ```
 * {
 *    "transport": "<protocol>",
 *    "credentials": 42,
 *    "members": [
 *        { "rank": 12, "address": "<some-address>", "provider_id": 1234 },
 *        ...
 *    ],
 *    "metadata": {
 *        "key": "value"
 *        ...
 *    }
 * }
 * ```
 * The "transport" field contains the Mercury transport method used (e.g. ofi+tcp),
 * the "credentials" field contains a credential token if communication with group
 * members requires one. The "members" field is an array of members, each described
 * with its address and provider ID. The "metadata" field contains any key/value pairs
 * relevant to the group's usage.
 */
flock_return_t flock_group_handle_create_from_file(
        flock_client_t client,
        const char* filename,
        uint32_t mode,
        flock_group_handle_t* handle);

/**
 * @brief Creates a FLOCK group handle from a serialized view.
 * This serialized view must have been generated using
 * flock_group_handle_serialize.
 *
 * @param[in] client FLOCK client responsible for the group handle
 * @param[in] addr Mercury address of the provider
 * @param[in] provider_id id of the provider
 * @param[in] mode Optional mode
 * @param[out] handle group handle
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_group_handle_create_from_serialized(
        flock_client_t client,
        const char* serialized_view,
        size_t view_size,
        uint32_t mode,
        flock_group_handle_t* handle);

/**
 * @brief Increments the reference counter of a group handle.
 *
 * @param handle group handle
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_group_handle_ref_incr(
        flock_group_handle_t handle);

/**
 * @brief Releases the group handle. This will decrement the
 * reference counter, and free the group handle if the reference
 * counter reaches 0.
 *
 * @param[in] handle group handle to release.
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_group_handle_release(flock_group_handle_t handle);

/**
 * @brief Serialize the current group handle and pass the resulting
 * string representation to the serializer function pointer.
 *
 * @param handle Group handle
 * @param serializer Serializer function
 * @param context Context to pass as first argument of the serializer function
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_group_serialize(
        flock_group_handle_t handle,
        void (*serializer)(void*, const char*, size_t),
        void* context);

/**
 * @brief Get the size of the group.
 *
 * @warning The size of the group is NOT the current number of processes,
 * it is N where N-1 is the maximum rank that a process of the group has
 * ever been associated with.
 *
 * This function will not incure any communication. The size returned
 * is the last size known to this client.
 *
 * @param[in] handle Group handle
 * @param[out] size Current known size
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_group_size(
        flock_group_handle_t handle,
        size_t* size);

/**
 * @brief Get the currently known number of live members.
 *
 * @param handle Group handle
 * @param count Number of live members known
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_group_live_member_count(
        flock_group_handle_t handle,
        size_t* count);

/**
 * @brief Function type used to access member information.
 *
 * @param void* User-provided context
 * @param size_t Member rank
 * @param const char* Address of the member
 * @param uint16_t Provider ID of the member
 *
 * @return true to continue iterating, false to break
 */
typedef bool (*flock_member_access_fn)(void*, size_t, const char*, uint16_t);

/**
 * @brief Iterate over the members of the group. The iteration
 * is garanteed to be from rank 0 to N-1, where N is the size of the group,
 * skipping ranks that are not associated with a live member.
 *
 * @param handle Group handle
 * @param access_fn Function to call on each member
 * @param context Context to pass to the callback
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_group_member_iterate(
        flock_group_handle_t handle,
        flock_member_access_fn access_fn,
        void* context);

/**
 * @brief Get the address of a member at a given rank.
 *
 * @important The caller is responsible for calling margo_addr_free
 * on the resuling hg_addr_t.
 *
 * If no member exist at that rank, the address will be set to HG_ADDR_NULL
 * and the function will return FLOCK_ERR_NO_MEMBER.
 *
 * @param[in] handle Group handle
 * @param[in] rank Rank of the process
 * @param[out] address Address of the process
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_group_member_get_address(
        flock_group_handle_t handle,
        size_t rank,
        hg_addr_t* address);

/**
 * @brief Get the address of a member at a given rank, as a string.
 *
 * @important The caller is responsible for calling free on the address.
 *
 * If no member exist at that rank, the address will be set to NULL
 * and the function will return FLOCK_ERR_NO_MEMBER.
 *
 * @param[in] handle Group handle
 * @param[in] rank Rank of the process
 * @param[out] address Address of the process
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_group_member_get_address_string(
        flock_group_handle_t handle,
        size_t rank,
        char** address);

/**
 * @brief Get the provider ID of a member at a given rank.
 *
 * If no member exist at that rank, the function will return FLOCK_ERR_NO_MEMBER.
 *
 * @param[in] handle Group handle
 * @param[in] rank Rank of the process
 * @param[out] provider_id Provider ID
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_group_member_get_provider_id(
        flock_group_handle_t handle,
        size_t rank,
        uint16_t* provider_id);

/**
 * @brief Get the rank of a member from its address and provider ID.
 *
 * @param[in] handle Group handle
 * @param[in] address Address of the member
 * @param[in] provider_id Provider ID of the member
 * @param[out] rank Rank of the member
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_group_member_get_rank(
        flock_group_handle_t handle,
        const char* address,
        uint16_t provider_id,
        size_t* rank);

/**
 * @brief Function type used to access the metadata of the group.
 *
 * @param void* User-provided context
 * @param const char* Metadata key
 * @param const char* Metadata value
 *
 * @return true to continue iterating, false to break
 */
typedef bool (*flock_metadata_access_fn)(void*, const char*, const char*);

/**
 * @brief Iterate over the metadata associated with the group.
 *
 * @param handle Group handle
 * @param access_fn Function to call on each key/value pair
 * @param context Context to pass to the callback
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_group_metadata_iterate(
        flock_group_handle_t handle,
        flock_metadata_access_fn access_fn,
        void* context);

/**
 * @brief Get the value associated with a given key and pass
 * the key/value pair to the provided access function.
 *
 * If no value is found associated with the specified key,
 * the callback will be called with NULL as the value.
 *
 * @param handle Group handle
 * @param key Metadata key
 * @param access_fn Function to call on the metadata
 * @param context Context to pass to the callback
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_group_metadata_access(
        flock_group_handle_t handle,
        const char* key,
        flock_metadata_access_fn access_fn,
        void* context);

/**
 * @brief Send a key/value pair to be added in the metadata of the
 * group. How this information will be propagated in the group depends
 * on the group's implementation. There is no guarantee that the metadata
 * is added upon completion of this function.
 *
 * @param handle Group handle
 * @param key Key to add
 * @param value Value to add
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_group_metadata_set(
        flock_group_handle_t handle,
        const char* key,
        const char* value);

/**
 * @brief Update the cached, internal view of the group
 * by contacting one (or more) of its members.
 *
 * If req != NULL, the operation will be non-blocking
 *
 * @param handle Group handle.
 * @param req Optional request pointer.
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_group_update_view(
        flock_group_handle_t handle,
        flock_request_t* req);

/**
 * @brief Wait for completion of a request.
 *
 * @param req Request to wait on.
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_request_wait(flock_request_t req);

/**
 * @brief Test for completion of a request.
 *
 * @param req Request to test.
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_request_test(flock_request_t req, bool* completed);

/**
 * @brief Subscribe to updates from the group. The consistency of such
 * updates depend on the backend implementation of the group's fault detection protocol.
 *
 * @important The client's margo_instance_id must have been initialized as a server
 * for this functionality to be available.
 *
 * @param handle Group handle
 * @param member_update_fn Function to call when a member is updated
 * @param metadata_update_fn Function to call when a metadata is updated
 * @param context Context to pass to the above functions
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_group_subscribe(
        flock_group_handle_t handle,
        flock_membership_update_fn member_update_fn,
        flock_metadata_update_fn metadata_update_fn,
        void* context);

/**
 * @brief Stop being notified about updates from this group.
 *
 * @param handle Group handle
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_group_unsubscribe(
        flock_group_handle_t handle);

#ifdef __cplusplus
}
#endif

#endif
