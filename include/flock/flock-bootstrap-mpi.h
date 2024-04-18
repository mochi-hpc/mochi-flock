/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FLOCK_BOOTSTRAP_MPI_H
#define __FLOCK_BOOTSTRAP_MPI_H

#include <string.h>
#include <margo.h>
#include <flock/flock-group-view.h>
#include <mpi.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Initialize a view from an MPI communicator.
 * This is a collective operation across the members of the communicator.
 *
 * @param[in] mid Margo instance ID
 * @param[in] provider_id Provider ID of the provider to register on the calling process.
 * @param[in] comm Communicator
 * @param[out] view View
 *
 * @return FLOCK_SUCCESS or other error codes.
 */
flock_return_t flock_group_view_init_from_mpi(
        margo_instance_id mid,
        uint16_t provider_id,
        MPI_Comm comm,
        flock_group_view_t* view);


#ifdef __cplusplus
}
#endif

#endif
