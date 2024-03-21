/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __PROVIDER_H
#define __PROVIDER_H

#include <margo.h>
#include <json-c/json.h>
#include "flock/flock-backend.h"

typedef struct flock_group {
    flock_backend_impl* fn;  // pointer to function mapping for this backend
    void*               ctx; // context required by the backend
} flock_group;

typedef struct flock_provider {
    /* Margo/Argobots/Mercury environment */
    margo_instance_id   mid;         // Margo instance
    uint16_t            provider_id; // Provider id
    ABT_pool            pool;        // Pool on which to post RPC requests
    /* Resource */
    flock_group* group;
    /* RPC identifiers for admins */
    hg_id_t create_group_id;
    hg_id_t destroy_group_id;
    /* RPC identifiers for clients */
    hg_id_t sum_id;
    /* ... add other RPC identifiers here ... */
} flock_provider;

#endif
