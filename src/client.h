/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef _CLIENT_H
#define _CLIENT_H

#include "types.h"
#include "alpha/alpha-client.h"
#include "alpha/alpha-resource.h"

typedef struct alpha_client {
   margo_instance_id mid;
   hg_id_t           sum_id;
   uint64_t          num_resource_handles;
} alpha_client;

typedef struct alpha_resource_handle {
    alpha_client_t      client;
    hg_addr_t           addr;
    uint16_t            provider_id;
    uint64_t            refcount;
} alpha_resource_handle;

#endif
