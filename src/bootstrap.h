/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FLOCK_MPI_H
#define __FLOCK_MPI_H

#include <string.h>
#include <margo.h>
#include <flock/flock-group-view.h>
#include <flock/flock-group.h>
#include <flock/flock-server.h>
#ifdef ENABLE_MPI
#include <mpi.h>
#endif

static inline flock_return_t flock_group_view_init_from_self(
    margo_instance_id mid,
    uint16_t provider_id,
    flock_group_view_t* view)
{
    char      self_addr_str[256];
    hg_size_t self_addr_size = 256;
    hg_addr_t self_addr      = HG_ADDR_NULL;
    hg_return_t hret         = HG_SUCCESS;
    flock_return_t ret       = FLOCK_SUCCESS;

    hret = margo_addr_self(mid, &self_addr);
    if(hret != HG_SUCCESS) {
        ret = FLOCK_ERR_FROM_MERCURY;
        goto finish;
    }
    hret = margo_addr_to_string(mid, self_addr_str, &self_addr_size, self_addr);
    if(hret != HG_SUCCESS) {
        ret = FLOCK_ERR_FROM_MERCURY;
        goto finish;
    }

    flock_group_view_clear(view);

    if(!flock_group_view_add_member(view, 0, provider_id, self_addr_str)) {
        ret = FLOCK_ERR_ALLOCATION;
        goto finish;
    }

finish:
    margo_addr_free(mid, self_addr);
    return ret;
}

static inline flock_return_t flock_group_view_init_from_view(
    flock_group_view_t* initial_view,
    flock_group_view_t* view)
{
    flock_group_view_clear(view);
    FLOCK_GROUP_VIEW_MOVE(initial_view, view);
    if(view->members.size == 0)
        return FLOCK_ERR_NO_MEMBER;
    else
        return FLOCK_SUCCESS;
}

static inline flock_return_t flock_group_view_init_from_handle(
        flock_group_handle_t group,
        flock_group_view_t* view)
{
    flock_return_t ret = FLOCK_SUCCESS;
    ret = flock_group_update_view(group, NULL);
    if(ret != FLOCK_SUCCESS) goto finish;

    flock_group_view_clear(view);

    ret = flock_group_get_view(group, view);
    if(ret != FLOCK_SUCCESS) goto finish;

finish:
    return ret;
}

static inline flock_return_t flock_group_view_init_from_file(
        margo_instance_id mid,
        const char* filename,
        flock_group_view_t* view)
{
    flock_return_t       ret    = FLOCK_SUCCESS;
    flock_client_t       client = FLOCK_CLIENT_NULL;
    flock_group_handle_t group  = FLOCK_GROUP_HANDLE_NULL;

    ret = flock_client_init(mid, ABT_POOL_NULL, &client);
    if(ret != FLOCK_SUCCESS) goto finish;

    ret = flock_group_handle_create_from_file(client, filename, 0, &group);
    if(ret != FLOCK_SUCCESS) goto finish;

    ret = flock_group_update_view(group, NULL);
    if(ret != FLOCK_SUCCESS) goto finish;

    flock_group_view_clear(view);

    ret = flock_group_get_view(group, view);
    if(ret != FLOCK_SUCCESS) goto finish;

finish:
    flock_group_handle_release(group);
    flock_client_finalize(client);
    return ret;
}

#ifdef ENABLE_MPI
static inline flock_return_t flock_group_view_init_from_mpi(
        margo_instance_id mid,
        uint16_t provider_id,
        MPI_Comm comm,
        flock_group_view_t* view)
{
    char      self_addr_str[256];
    hg_size_t self_addr_size = 256;
    hg_addr_t self_addr      = HG_ADDR_NULL;
    hg_return_t hret         = HG_SUCCESS;
    flock_return_t ret       = FLOCK_SUCCESS;
    uint16_t* provider_ids   = NULL;
    char* addresses_buf      = NULL;
    int size, rank, mret;

    hret = margo_addr_self(mid, &self_addr);
    if(hret != HG_SUCCESS) {
        ret = FLOCK_ERR_FROM_MERCURY;
        goto finish;
    }
    hret = margo_addr_to_string(mid, self_addr_str, &self_addr_size, self_addr);
    if(hret != HG_SUCCESS) {
        ret = FLOCK_ERR_FROM_MERCURY;
        goto finish;
    }

    mret = MPI_Comm_rank(comm, &rank);
    if(mret != 0) {
        ret = FLOCK_ERR_FROM_MPI;
        goto finish;
    }
    mret = MPI_Comm_size(comm, &size);
    if(mret != 0) {
        ret = FLOCK_ERR_FROM_MPI;
        goto finish;
    }

    provider_ids = (uint16_t*)malloc(size*sizeof(provider_id));
    if(!provider_ids) {
        ret = FLOCK_ERR_ALLOCATION;
        goto finish;
    }
    addresses_buf = (char*)malloc(size*256);
    if(!addresses_buf) {
        ret = FLOCK_ERR_ALLOCATION;
        goto finish;
    }

    mret = MPI_Allgather(&provider_id, 1, MPI_UINT16_T, provider_ids, 1, MPI_UINT16_T, comm);
    if(mret != 0) {
        ret = FLOCK_ERR_FROM_MPI;
        goto finish;
    }

    mret = MPI_Allgather(self_addr_str, 256, MPI_CHAR, addresses_buf, 256, MPI_CHAR, comm);
    if(mret != 0) {
        ret = FLOCK_ERR_FROM_MPI;
        goto finish;
    }

    flock_group_view_clear(view);

    for(int i = 0; i < size; ++i) {
        if(!flock_group_view_add_member(view, (uint64_t)i, provider_ids[i], addresses_buf + 256*i)) {
            ret = FLOCK_ERR_ALLOCATION;
            goto finish;
        }
    }

finish:
    free(provider_ids);
    free(addresses_buf);
    margo_addr_free(mid, self_addr);
    return ret;
}
#endif

static inline flock_return_t flock_bootstrap_group_view(
        const char* method,
        margo_instance_id mid,
        uint16_t provider_id,
        struct flock_provider_args* args,
        const char* filename,
        flock_group_view_t* view)
{
    if(strcmp(method, "self") == 0) {
        return flock_group_view_init_from_self(mid, provider_id, view);
    } else if(strcmp(method, "view") == 0) {
        return flock_group_view_init_from_view(args->bootstrap.initial_view, view);
    } else if(strcmp(method, "mpi") == 0) {
#ifdef ENABLE_MPI
        MPI_Comm comm = MPI_COMM_WORLD;
        if(args->bootstrap.mpi_comm)
            comm = (MPI_Comm)args->bootstrap.mpi_comm;
        return flock_group_view_init_from_mpi(mid, provider_id, comm, view);
#else
        margo_error(mid, "[flock] Unknown bootstrap method \"%s\"", method);
        return FLOCK_ERR_OP_UNSUPPORTED;
#endif
    } else if(strcmp(method, "join") == 0) {
        return flock_group_view_init_from_handle(
            args->bootstrap.join_group, view);
    } else if(strcmp(method, "file") == 0) {
        return flock_group_view_init_from_file(mid, filename, view);
    }
    margo_error(mid, "[flock] Unknown bootstrap method \"%s\"", method);
    return FLOCK_ERR_INVALID_ARGS;
}

#endif
