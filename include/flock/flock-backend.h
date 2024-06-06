/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FLOCK_BACKEND_H
#define __FLOCK_BACKEND_H

#include <margo.h>
#include <flock/flock-common.h>
#include <flock/flock-group-view.h>

#ifdef __cplusplus
extern "C" {
#endif

struct json_object;

/**
 * @brief Initialization arguments.
 *
 * @note The backend's init_group can call json_object_get on the
 * config to increase its reference count and keep it internally
 * (the provider does not modify it).
 *
 * @note The backend's init_group can move the initial_view internally.
 * If it does it, it should memset the flock_backend_init_args's initial_view
 * field to 0 so that the provider does not free it.
 *
 * @note The provided member_update_callback and metadata_update_callback
 * need to be called by the backend whenever a member leaves or joins,
 * and whenever the metadata is updated, respectively. When called, these
 * function must be passed the provided callback_context.
 */
typedef struct flock_backend_init_args {
    margo_instance_id          mid;
    uint16_t                   provider_id;
    ABT_pool                   pool;
    struct json_object*        config;
    bool                       join;
    flock_group_view_t         initial_view;
    flock_membership_update_fn member_update_callback;
    flock_metadata_update_fn   metadata_update_callback;
    void*                      callback_context;
} flock_backend_init_args_t;


/**
 * @brief Allocates and initializes the state of the backend.
 *
 * @param[in] flock_backend_init_args_t* Initialization arguments.
 * @param[out] void** Pointer to the allocated state.
 *
 * @note This function may move some of the fields of the flock_backend_init_args_t
 * (see comments about flock_backend_init_args_t above).
 */
typedef flock_return_t (*flock_backend_init_fn)(flock_backend_init_args_t* args, void**);

/**
 * @brief Finalizes and deallocate the state of the backend.
 *
 * @param void* Pointer to the backend's state.
 */
typedef flock_return_t (*flock_backend_finalize_fn)(void*);

/**
 * @brief Get the config of the backend and pass it to the
 * provided function pointer.
 *
 * @param void* Pointer to the backend's state.
 * @param void (*)(void*, const struct json_object*) Function to call on the config.
 * @param void* Context to pass to the function.
 */
typedef flock_return_t (*flock_backend_get_config_fn)(void*, void (*)(void*, const struct json_object*), void*);

/**
 * @brief Get the group view held by the backend.
 *
 * @param void* Pointer to the backend's state.
 * @param void (*)(void*, const flock_group_view_t*) Function to call on the group view.
 * @param void* Context to pass to the function.
 *
 * @important This function should NOT lock the view using the view's mtx field.
 */
typedef flock_return_t (*flock_backend_get_view_fn)(void*, void (*)(void*, const flock_group_view_t*), void*);

/**
 * @brief Add metadata to the backend.
 *
 * @param void* Pointer to the backend's state.
 * @param const char* Key (null-terminated).
 * @param const char* Value (null-terminated).
 */
typedef flock_return_t (*flock_backend_add_metadata_fn)(void*, const char*, const char*);

/**
 * @brief Remove metadata from the backend.
 *
 * @param void* Pointer to the backend's state.
 * @param const char* Key (null-terminated).
 */
typedef flock_return_t (*flock_backend_remove_metadata_fn)(void*, const char*);

/**
 * @brief Implementation of an FLOCK backend.
 */
typedef struct flock_backend_impl {
    // backend name
    const char* name;
    // backend functions
    flock_backend_init_fn            init_group;
    flock_backend_finalize_fn        destroy_group;
    flock_backend_get_config_fn      get_config;
    flock_backend_get_view_fn        get_view;
    flock_backend_add_metadata_fn    add_metadata;
    flock_backend_remove_metadata_fn remove_metadata;
} flock_backend_impl;

/**
 * @brief Registers a backend implementation.
 *
 * Note: the backend implementation will not be copied; it is
 * therefore important that it stays valid in memory while
 * in use by any flock provider.
 *
 * Important: up to 64 backend implementations may be registered,
 * including the ones provided internally.
 *
 * @param backend_impl backend implementation.
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_register_backend(flock_backend_impl* backend_impl);

#ifdef __cplusplus
}
#endif

#endif
