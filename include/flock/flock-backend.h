/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FLOCK_BACKEND_H
#define __FLOCK_BACKEND_H

#include <margo.h>
#include <flock/flock-common.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct flock_provider* flock_provider_t;

typedef flock_return_t (*flock_backend_create_fn)(margo_instance_id, flock_provider_t, const char*, void**);
typedef flock_return_t (*flock_backend_destroy_fn)(void*);
typedef char* (*flock_backend_get_config_fn)(void*);

/**
 * @brief Implementation of an FLOCK backend.
 */
typedef struct flock_backend_impl {
    // backend name
    const char* name;
    // backend management functions
    flock_backend_create_fn     create_group;
    flock_backend_destroy_fn    destroy_group;
    flock_backend_get_config_fn get_config;
    // RPC functions
    int32_t (*sum)(void*, int32_t, int32_t);
    // ... add other functions here
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
