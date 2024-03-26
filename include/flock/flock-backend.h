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

typedef struct flock_backend_init_args {
    margo_instance_id   mid;
    uint16_t            provider_id;
    ABT_pool            pool;
    struct json_object* config;
} flock_backend_init_args_t;


typedef flock_return_t (*flock_backend_init_fn)(const flock_backend_init_args_t* args, void**);
typedef flock_return_t (*flock_backend_finalize_fn)(void*);
typedef flock_return_t (*flock_backend_get_config_fn)(void*, void (*)(void*, const struct json_object*), void*);
typedef flock_return_t (*flock_backend_get_view_fn)(void*, void (*)(void*, const flock_group_view_t*), void*);
typedef flock_return_t (*flock_backend_add_metadata_fn)(void*, const char*, const char*);
typedef flock_return_t (*flock_backend_remove_metadata_fn)(void*, const char*);
typedef flock_return_t (*flock_backend_add_member_fn)(void*, uint64_t, const char*, uint16_t);
typedef flock_return_t (*flock_backend_remove_member_fn)(void*, uint64_t);

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
    flock_backend_add_member_fn      add_member;
    flock_backend_remove_member_fn   remove_member;
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
