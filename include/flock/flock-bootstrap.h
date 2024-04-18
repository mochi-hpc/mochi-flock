/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FLOCK_BOOTSTRAP_H
#define __FLOCK_BOOTSTRAP_H

#include <string.h>
#include <margo.h>
#include <flock/flock-group-view.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Initialize a group view with the calling process
 * (with the specified provider ID) as the only member.
 *
 * @param[in] mid Margo instance ID
 * @param[in] provider_id Provider ID of the only member
 * @param[out] view View
 *
 * @return FLOCK_SUCCESS or other error code.
 */
flock_return_t flock_group_view_init_from_self(
        margo_instance_id mid,
        uint16_t provider_id,
        flock_group_view_t* view);

/**
 * @brief Initialize a group view by reading it from a file.
 *
 * @param[in] mid Margo instance ID
 * @param[in] filename File containing the view
 * @param[out] view View
 *
 * @return FLOCK_SUCCESS or other error code.
 */
flock_return_t flock_group_view_init_from_file(
        margo_instance_id mid,
        const char* filename,
        flock_group_view_t* view);

#ifdef __cplusplus
}
#endif

#endif
