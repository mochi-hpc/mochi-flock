/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __PROVIDER_H
#define __PROVIDER_H

#include <margo.h>
#include <json-c/json.h>
#include "alpha/alpha-backend.h"

typedef struct alpha_resource {
    alpha_backend_impl* fn;  // pointer to function mapping for this backend
    void*               ctx; // context required by the backend
} alpha_resource;

typedef struct alpha_provider {
    /* Margo/Argobots/Mercury environment */
    margo_instance_id   mid;         // Margo instance
    uint16_t            provider_id; // Provider id
    ABT_pool            pool;        // Pool on which to post RPC requests
    /* Resource */
    alpha_resource* resource;
    /* RPC identifiers for admins */
    hg_id_t create_resource_id;
    hg_id_t destroy_resource_id;
    /* RPC identifiers for clients */
    hg_id_t sum_id;
    /* ... add other RPC identifiers here ... */
} alpha_provider;

#endif
