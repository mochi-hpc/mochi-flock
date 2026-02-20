/*
 * (C) 2026 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FLOCK_GATEWAY_H
#define __FLOCK_GATEWAY_H

#include <margo.h>

#ifdef __cplusplus
extern "C" {
#endif

struct json_object;

/**
 * @brief Initialization arguments.
 *
 * @note The gateway's init_gateway can call json_object_get on the
 * config to increase its reference count and keep it internally
 * (the provider does not modify it).
 */
typedef struct flock_gateway_init_args {
    margo_instance_id   mid;
    uint16_t            provider_id;
    ABT_pool            pool;
    struct json_object* config;
} flock_gateway_init_args_t;

/**
 * @brief Allocates and initializes the state of a gateway.
 *
 * @param[in] flock_gateway_init_args_t* Initialization arguments.
 * @param[out] void** Pointer to the allocated state.
 *
 * @note This function may move some of the fields of the flock_gateway_init_args_t
 * (see comments about flock_gateway_init_args_t above).
 */
typedef flock_return_t (*flock_gateway_init_fn)(flock_gateway_init_args_t* args, void**);

/**
 * @brief Finalizes and deallocate the state of the gateway.
 *
 * @param void* Pointer to the backend's state.
 */
typedef flock_return_t (*flock_gateway_finalize_fn)(void*);

/**
 * @brief Get the config of the gateway and pass it to the
 * provided function pointer.
 *
 * @param void* Pointer to the gateway's state.
 * @param void (*)(void*, const struct json_object*) Function to call on the config.
 * @param void* Context to pass to the function.
 */
typedef flock_return_t (*flock_gateway_get_config_fn)(void*, void (*)(void*, const struct json_object*), void*);

/**
 * @brief Get the public address of this process according to the gateway.
 *
 * @param void* Pointer to the gateway's state.
 * @return const char* Public address as a null-terminated string.
 */
typedef const char* (*flock_gateway_get_public_address_fn)(void*);

/**
 * @brief Get the local address of this process.
 *
 * @param void* Pointer to the gateway's state.
 * @return const char* Local address as a null-terminated string.
 */
typedef const char* (*flock_gateway_get_local_address_fn)(void*);

/**
 * @brief Implementation of an FLOCK gateway.
 */
typedef struct flock_gateway_impl {
    // gateway name
    const char* name;
    // gateway functions
    flock_gateway_init_fn               init_gateway;
    flock_gateway_finalize_fn           destroy_gateway;
    flock_gateway_get_config_fn         get_config;
    flock_gateway_get_public_address_fn get_public_address;
    flock_gateway_get_local_address_fn  get_local_address;
} flock_gateway_impl;

/**
 * @brief Registers a gateway implementation.
 *
 * Note: the gateway implementation will not be copied; it is
 * therefore important that it stays valid in memory while
 * in use by any flock provider.
 *
 * Important: up to 64 gateway implementations may be registered,
 * including the ones provided internally.
 *
 * @param gateway_impl gateway implementation.
 *
 * @return FLOCK_SUCCESS or error code defined in flock-common.h
 */
flock_return_t flock_register_gateway(flock_gateway_impl* gateway_impl);

#ifdef __cplusplus
}
#endif

#endif
