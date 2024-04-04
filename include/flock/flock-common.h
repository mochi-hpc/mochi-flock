/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FLOCK_COMMON_H
#define __FLOCK_COMMON_H

#include <stdint.h>
#include <margo.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum flock_update_t {
    FLOCK_MEMBER_JOINED,
    FLOCK_MEMBER_LEFT,
    FLOCK_MEMBER_DIED,
} flock_update_t;

#define FLOCK_MODE_INIT_UPDATE 0x1 /* Update the group on initialization */
#define FLOCK_MODE_SUBSCRIBE   0x2 /* Subscribe to the group on initialization */

/**
 * @brief Error codes that can be returned by FLOCK functions.
 */
typedef enum flock_return_t {
    FLOCK_SUCCESS,
    FLOCK_ERR_ALLOCATION,        /* Allocation error */
    FLOCK_ERR_INVALID_ARGS,      /* Invalid argument */
    FLOCK_ERR_INVALID_PROVIDER,  /* Invalid provider id */
    FLOCK_ERR_INVALID_GROUP,     /* Invalid group id */
    FLOCK_ERR_INVALID_BACKEND,   /* Invalid backend type */
    FLOCK_ERR_INVALID_CONFIG,    /* Invalid configuration */
    FLOCK_ERR_FROM_MERCURY,      /* Mercurt error */
    FLOCK_ERR_FROM_ARGOBOTS,     /* Argobots error */
    FLOCK_ERR_FROM_MPI,          /* MPI error */
    FLOCK_ERR_OP_UNSUPPORTED,    /* Unsupported operation */
    FLOCK_ERR_OP_FORBIDDEN,      /* Forbidden operation */
    FLOCK_ERR_NO_MEMBER,         /* No member at this rank */
    FLOCK_ERR_NO_METADATA,       /* Invalid metadata key */
    /* ... TODO add more error codes here if needed */
    FLOCK_ERR_OTHER              /* Other error */
} flock_return_t;

/**
 * @brief Type of function called when a member joins, leaves, or dies.
 *
 * @param void* User-provided context
 * @param flock_update_t Update type
 * @param size_t Rank of the member
 * @param const char* Address of the member
 * @param uint16_t Provider ID of the member
 */
typedef void (*flock_membership_update_fn)(void*, flock_update_t, size_t, const char*, uint16_t);

/**
 * @brief Type of function called when a key/value pair in the metadata of
 * a group is updated.
 *
 * @param void* User-provided context
 * @param const char* Metadata key
 * @param size_t Size of the metadata key
 * @param const char* Metadata value
 * @param size_t Size of the metadata value
 */
typedef void (*flock_metadata_update_fn)(void*, const char*, const char*);

#ifdef __cplusplus
}
#endif

#endif
