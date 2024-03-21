/*
 * (C) 2020 The University of Chicago
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
 * @brief Creates a FLOCK group handle.
 *
 * @param[in] client FLOCK client responsible for the group handle
 * @param[in] addr Mercury address of the provider
 * @param[in] provider_id id of the provider
 * @param[in] check If true, will send an RPC to check that the provider exists
 * @param[out] handle group handle
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_group_handle_create(
        flock_client_t client,
        hg_addr_t addr,
        uint16_t provider_id,
        bool check,
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
 * @brief Makes the target FLOCK group compute the sum of the
 * two numbers and return the result.
 *
 * @param[in] handle group handle.
 * @param[in] x first number.
 * @param[in] y second number.
 * @param[out] result resulting value.
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_compute_sum(
        flock_group_handle_t handle,
        int32_t x,
        int32_t y,
        int32_t* result);

#ifdef __cplusplus
}
#endif

#endif
