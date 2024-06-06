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
    FLOCK_MEMBER_JOINED, /* new member added */
    FLOCK_MEMBER_LEFT,   /* member with specified rank left */
    FLOCK_MEMBER_DIED,   /* member with specified rank died */
    FLOCK_MEMBER_MOVED,  /* member with specified rank changed address and/or provider id */
} flock_update_t;

#define FLOCK_MODE_INIT_UPDATE 0x1 /* Update the group on initialization */
#define FLOCK_MODE_SUBSCRIBE   0x2 /* Subscribe to the group on initialization */

#define FLOCK_RETURN_VALUES                              \
    X(FLOCK_SUCCESS, "Success")                          \
    X(FLOCK_ERR_ALLOCATION, "Allocation error")          \
    X(FLOCK_ERR_INVALID_ARGS, "Invalid argument")        \
    X(FLOCK_ERR_INVALID_PROVIDER, "Invalid provider id") \
    X(FLOCK_ERR_INVALID_GROUP, "Invalid group id")       \
    X(FLOCK_ERR_INVALID_BACKEND, "Invalid backend type") \
    X(FLOCK_ERR_INVALID_CONFIG, "Invalid configuration") \
    X(FLOCK_ERR_FROM_MERCURY, "Mercurt error")           \
    X(FLOCK_ERR_FROM_ARGOBOTS, "Argobots error")         \
    X(FLOCK_ERR_FROM_MPI, "MPI error")                   \
    X(FLOCK_ERR_OP_UNSUPPORTED, "Unsupported operation") \
    X(FLOCK_ERR_OP_FORBIDDEN, "Forbidden operation")     \
    X(FLOCK_ERR_NO_MEMBER, "No member at this rank")     \
    X(FLOCK_ERR_NO_METADATA, "Invalid metadata key")     \
    X(FLOCK_ERR_NOT_A_MEMBER, "Process is not member")   \
    X(FLOCK_ERR_RANK_USED, "Rank alread used")           \
    X(FLOCK_ERR_OTHER, "Other error")


/**
 * @brief Error codes that can be returned by FLOCK functions.
 */
#define X(__err__, __msg__) __err__,
typedef enum flock_return_t {
    FLOCK_RETURN_VALUES
} flock_return_t;
#undef X

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
