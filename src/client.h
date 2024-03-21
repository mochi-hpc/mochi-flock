/*
 * (C) 2020 The University of Chicago
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
   hg_id_t           sum_id;
   uint64_t          num_group_handles;
} flock_client;

typedef struct flock_group_handle {
    flock_client_t      client;
    hg_addr_t           addr;
    uint16_t            provider_id;
    uint64_t            refcount;
} flock_group_handle;

#endif
