/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FLOCK_COMMON_H
#define __FLOCK_COMMON_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Error codes that can be returned by FLOCK functions.
 */
typedef enum flock_return_t {
    FLOCK_SUCCESS,
    FLOCK_ERR_ALLOCATION,        /* Allocation error */
    FLOCK_ERR_INVALID_ARGS,      /* Invalid argument */
    FLOCK_ERR_INVALID_PROVIDER,  /* Invalid provider id */
    FLOCK_ERR_INVALID_GROUP,  /* Invalid group id */
    FLOCK_ERR_INVALID_BACKEND,   /* Invalid backend type */
    FLOCK_ERR_INVALID_CONFIG,    /* Invalid configuration */
    FLOCK_ERR_INVALID_TOKEN,     /* Invalid token */
    FLOCK_ERR_FROM_MERCURY,      /* Mercurt error */
    FLOCK_ERR_FROM_ARGOBOTS,     /* Argobots error */
    FLOCK_ERR_OP_UNSUPPORTED,    /* Unsupported operation */
    FLOCK_ERR_OP_FORBIDDEN,      /* Forbidden operation */
    /* ... TODO add more error codes here if needed */
    FLOCK_ERR_OTHER              /* Other error */
} flock_return_t;

#ifdef __cplusplus
}
#endif

#endif
