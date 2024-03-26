/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "types.h"
#include "client.h"
#include "flock/flock-client.h"

flock_return_t flock_client_init(margo_instance_id mid, ABT_pool pool, flock_client_t* client)
{
    flock_client_t c = (flock_client_t)calloc(1, sizeof(*c));
    if(!c) return FLOCK_ERR_ALLOCATION;

    c->mid = mid;
    if(pool == ABT_POOL_NULL)
        margo_get_handler_pool(mid, &pool);
    c->pool = pool;

    hg_bool_t flag;
    hg_id_t id;
    margo_registered_name(mid, "flock_update", &id, &flag);

    if(flag == HG_TRUE) {
        margo_registered_name(mid, "flock_update", &c->update_id, &flag);
    } else {
        c->update_id = MARGO_REGISTER(mid, "flock_update", void, update_out_t, NULL);
    }

    *client = c;
    return FLOCK_SUCCESS;
}

flock_return_t flock_client_finalize(flock_client_t client)
{
    if(client->num_group_handles != 0) {
        margo_warning(client->mid,
            "%ld group handles not released when flock_client_finalize was called",
            client->num_group_handles);
    }
    free(client);
    return FLOCK_SUCCESS;
}

flock_return_t flock_group_update(
        flock_group_handle_t handle,
        flock_request_t* req)
{
    hg_handle_t   h;
    update_out_t  out;
    hg_return_t hret;
    flock_return_t ret = FLOCK_SUCCESS;

    hret = margo_create(handle->client->mid, handle->addr, handle->client->update_id, &h);
    if(hret != HG_SUCCESS)
        return FLOCK_ERR_FROM_MERCURY;

    hret = margo_provider_forward(handle->provider_id, h, NULL);
    if(hret != HG_SUCCESS) {
        ret = FLOCK_ERR_FROM_MERCURY;
        goto finish;
    }

    hret = margo_get_output(h, &out);
    if(hret != HG_SUCCESS) {
        ret = FLOCK_ERR_FROM_MERCURY;
        goto finish;
    }

    FLOCK_GROUP_VIEW_LOCK(&handle->view);
    handle->view.members.size      = out.view.members.size;
    handle->view.members.capacity  = out.view.members.capacity;
    handle->view.members.data      = out.view.members.data;
    handle->view.metadata.size     = out.view.metadata.size;
    handle->view.metadata.capacity = out.view.metadata.capacity;
    handle->view.metadata.data     = out.view.metadata.data;
    FLOCK_GROUP_VIEW_UNLOCK(&handle->view);

    // Prevent margo_free_output from freeing the content we just moved
    out.view.members.size      = 0;
    out.view.members.capacity  = 0;
    out.view.members.data      = NULL;
    out.view.metadata.size     = 0;
    out.view.metadata.capacity = 0;
    out.view.metadata.data     = NULL;

    margo_free_output(h, &out);

finish:
    margo_destroy(h);
    return ret;
}
