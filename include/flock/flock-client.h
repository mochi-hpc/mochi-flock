/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FLOCK_CLIENT_H
#define __FLOCK_CLIENT_H

#include <margo.h>
#include <flock/flock-common.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct flock_client* flock_client_t;
#define FLOCK_CLIENT_NULL ((flock_client_t)NULL)

/**
 * @brief Creates a FLOCK client.
 *
 * @param[in] mid Margo instance
 * @param[out] client FLOCK client
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_client_init(margo_instance_id mid, flock_client_t* client);

/**
 * @brief Finalizes a FLOCK client.
 *
 * @param[in] client FLOCK client to finalize
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_client_finalize(flock_client_t client);

#ifdef __cplusplus
}
#endif

#endif
