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
#include <mpi.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief This helper function will populate a flock_group_view_t
 * by using the provided MPI communicator. This function is collective
 * across all the members of the communicator. Each member may provide
 * a different provider Id. The resulting view will associate to each
 * flock_member_t the same rank as in the MPI communicator.
 *
 * @important The provided view will be reset in the process, including
 * clearing any metadata attached to it.
 *
 * @param[in] mid Margo instance ID.
 * @param[in] provider_id Provider ID for the calling process.
 * @param[in] comm MPI communicator.
 * @param[out] view View to populate.
 *
 * @return FLOCK_SUCCESS or other error codes.
 */
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

#ifdef __cplusplus
}
#endif

#endif
