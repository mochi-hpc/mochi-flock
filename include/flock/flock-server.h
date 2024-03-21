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
 * @brief Register callbacks that the provider will call when
 * a member is updated or when the metadata is changed.
 *
 * @param provider Provider
 * @param member_update_fn Function to call when a member is updated
 * @param metadata_update_fn Function to call when a metadata is updated
 * @param context Context to pass to the above functions
 *
 * @note The context argument is what will uniquely identify this
 * callback registration and can be used in flock_provider_remove_update_callback
 * to deregister these callbacks. It is valid to call flock_provider_add_update_callbacks
 * multiple times with distinct contexts.
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_provider_add_update_callbacks(
        flock_provider_t provider,
        flock_membership_update_fn member_update_fn,
        flock_metadata_update_fn metadata_update_fn,
        void* context);

/**
 * @brief Remove the callbask associated with the given context.
 *
 * @param provider Provider
 * @param context Context
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_provider_remove_update_callbacks(
        flock_provider_t provider,
        void* context);

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
