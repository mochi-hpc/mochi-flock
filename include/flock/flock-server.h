/*
 * (C) 2024 The University of Chicago
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

/**
 * @brief Optional arguments to pass to the flock_provider_register function.
 *
 * The pool will default to the margo_instance_id's default handler pool.
 *
 * If NULL, the initial_view will default to a view that include only the current
 * provider, with rank set to 0. If provided, the provider will take ownership of
 * the view's content and reset it to empty for the caller.
 *
 * If not provided, the backend will default to the type specified in the
 * JSON configuration passed to flock_provider_register.
 */
struct flock_provider_args {
    ABT_pool            pool;
    flock_backend_impl* backend;
};

#define FLOCK_PROVIDER_ARGS_INIT { \
    /* .pool = */ ABT_POOL_NULL,   \
    /* .backend = */ NULL          \
}

/**
 * @brief Creates a new FLOCK provider, bootstrapping the group
 * from a common initial view. The initial view MUST contain the
 * calling provider.
 *
 * @warning Calling this function with a different view on each
 * provider will cause undefined behavior.
 *
 * The config parameter must have the following format.
 *
 * ```
 * {
 *     "type": "static", // or another backend type
 *     "group": {
 *          // backend-specified configuration
 *     }
 * }
 * ```
 *
 * @param[in] mid Margo instance
 * @param[in] provider_id provider id
 * @param[in] initial_view initial view
 * @param[in] config Configuration
 * @param[in] args optional argument structure
 * @param[out] provider provider
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_provider_bootstrap(
        margo_instance_id mid,
        uint16_t provider_id,
        flock_group_view_t* initial_view,
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
