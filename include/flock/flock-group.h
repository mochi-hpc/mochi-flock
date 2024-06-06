/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FLOCK_GROUP_H
#define __FLOCK_GROUP_H

#include <margo.h>
#include <flock/flock-common.h>
#include <flock/flock-group-view.h>
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
 * @param[in] addr Mercury address of one of the members
 * @param[in] provider_id id of the member
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
 *    "members": [
 *        { "address": "<some-address>", "provider_id": 1234 },
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
 * @brief Copy the internal view. The resulting view should be freed
 * by the caller using flock_group_view_clear.
 *
 * @param[in] handle Group handle
 * @param[out] view View
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_group_get_view(
        flock_group_handle_t handle,
        flock_group_view_t* view);

/**
 * @brief Access the group's internal view without copying it,
 * by passing a function pointer. The view is locked until the
 * access function returns.
 *
 * @param handle Group handle
 * @param access Access function pointer
 * @param uargs Argument for the user-provided function pointer
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_group_access_view(
        flock_group_handle_t handle,
        void (*access)(void* uargs, const flock_group_view_t* view),
        void* uargs);

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
