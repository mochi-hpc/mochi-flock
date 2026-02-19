/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __PROVIDER_H
#define __PROVIDER_H

#include <margo.h>
#include <json-c/json.h>
#include "flock/flock-backend.h"
#include "flock/flock-gateway.h"

typedef struct flock_group {
    flock_backend_impl* fn;  // pointer to function mapping for this backend
    void*               ctx; // context required by the backend
} flock_group;

typedef struct flock_gateway {
    flock_gateway_impl* fn;
    void*               ctx;
} flock_gateway;

typedef struct update_callback* update_callback_t;
struct update_callback {
    flock_membership_update_fn member_cb;
    flock_metadata_update_fn   metadata_cb;
    void*                      args;
    update_callback_t          next;
};

typedef struct flock_provider {
    /* Margo/Argobots/Mercury environment */
    margo_instance_id   mid;           // Margo instance
    uint16_t            provider_id;   // Provider id
    ABT_pool            pool;          // Pool on which to post RPC requests
    char*               filename;      // Default group file name
    /* Gateway implementation */
    flock_gateway* gateway;
    /* Group implementation */
    flock_group* group;
    /* List of registered membership and metadata callbacks */
    update_callback_t update_callbacks;
    ABT_rwlock        update_callbacks_lock;
    /* RPC identifiers for clients */
    hg_id_t get_view_id;
    /* ... add other RPC identifiers here ... */
} flock_provider;

#endif
