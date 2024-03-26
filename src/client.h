/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef _CLIENT_H
#define _CLIENT_H

#include "types.h"
#include "flock/flock-client.h"
#include "flock/flock-group.h"

typedef struct flock_client {
   margo_instance_id mid;
   ABT_pool          pool;
   hg_id_t           update_id;
   uint64_t          num_group_handles;
} flock_client;

typedef struct flock_group_handle {
    flock_client_t client;
    uint64_t       refcount;
    // Bellow are the address and provider ID of the member
    // that will be contacted in priority for updates
    hg_addr_t      addr;
    uint16_t       provider_id;
    // Group view
    flock_group_view_t view;
    // Credentials
    int64_t        credentials;
} flock_group_handle;

#endif
