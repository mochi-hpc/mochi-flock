/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __ALPHA_CLIENT_H
#define __ALPHA_CLIENT_H

#include <margo.h>
#include <alpha/alpha-common.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct alpha_client* alpha_client_t;
#define ALPHA_CLIENT_NULL ((alpha_client_t)NULL)

/**
 * @brief Creates a ALPHA client.
 *
 * @param[in] mid Margo instance
 * @param[out] client ALPHA client
 *
 * @return ALPHA_SUCCESS or error code defined in alpha-common.h
 */
alpha_return_t alpha_client_init(margo_instance_id mid, alpha_client_t* client);

/**
 * @brief Finalizes a ALPHA client.
 *
 * @param[in] client ALPHA client to finalize
 *
 * @return ALPHA_SUCCESS or error code defined in alpha-common.h
 */
alpha_return_t alpha_client_finalize(alpha_client_t client);

#ifdef __cplusplus
}
#endif

#endif
