/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __ALPHA_BACKEND_H
#define __ALPHA_BACKEND_H

#include <margo.h>
#include <alpha/alpha-common.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct alpha_provider* alpha_provider_t;

typedef alpha_return_t (*alpha_backend_create_fn)(margo_instance_id, alpha_provider_t, const char*, void**);
typedef alpha_return_t (*alpha_backend_destroy_fn)(void*);
typedef char* (*alpha_backend_get_config_fn)(void*);

/**
 * @brief Implementation of an ALPHA backend.
 */
typedef struct alpha_backend_impl {
    // backend name
    const char* name;
    // backend management functions
    alpha_backend_create_fn     create_resource;
    alpha_backend_destroy_fn    destroy_resource;
    alpha_backend_get_config_fn get_config;
    // RPC functions
    int32_t (*sum)(void*, int32_t, int32_t);
    // ... add other functions here
} alpha_backend_impl;

/**
 * @brief Registers a backend implementation.
 *
 * Note: the backend implementation will not be copied; it is
 * therefore important that it stays valid in memory while
 * in use by any alpha provider.
 *
 * Important: up to 64 backend implementations may be registered,
 * including the ones provided internally.
 *
 * @param backend_impl backend implementation.
 *
 * @return ALPHA_SUCCESS or error code defined in alpha-common.h
 */
alpha_return_t alpha_register_backend(alpha_backend_impl* backend_impl);

#ifdef __cplusplus
}
#endif

#endif
