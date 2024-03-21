/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FLOCK_SERVER_H
#define __FLOCK_SERVER_H

#include <flock/flock-common.h>
#include <flock/flock-backend.h>
#include <margo.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOCK_ABT_POOL_DEFAULT ABT_POOL_NULL

typedef struct flock_provider* flock_provider_t;
#define FLOCK_PROVIDER_NULL ((flock_provider_t)NULL)
#define FLOCK_PROVIDER_IGNORE ((flock_provider_t*)NULL)

struct flock_provider_args {
    ABT_pool            pool;    // Pool used to run RPCs
    flock_backend_impl* backend; // Type of backend, will take priority over the "type" field in config
    // ...
};

#define FLOCK_PROVIDER_ARGS_INIT { \
    /* .pool = */ ABT_POOL_NULL, \
    /* .backend = */ NULL \
}

/**
 * @brief Creates a new FLOCK provider. If FLOCK_PROVIDER_IGNORE
 * is passed as last argument, the provider will be automatically
 * destroyed when calling margo_finalize.
 *
 * @param[in] mid Margo instance
 * @param[in] provider_id provider id
 * @param[in] args argument structure
 * @param[out] provider provider
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_provider_register(
        margo_instance_id mid,
        uint16_t provider_id,
        const char* config,
        const struct flock_provider_args* args,
        flock_provider_t* provider);

/**
 * @brief Destroys the Alpha provider and deregisters its RPC.
 *
 * @param[in] provider Alpha provider
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_provider_destroy(
        flock_provider_t provider);

/**
 * @brief Returns a JSON-formatted configuration of the provider.
 *
 * The caller is responsible for freeing the returned pointer.
 *
 * @param provider Alpha provider
 *
 * @return a heap-allocated JSON string or NULL in case of an error.
 */
char* flock_provider_get_config(
        flock_provider_t provider);

#ifdef __cplusplus
}
#endif

#endif
