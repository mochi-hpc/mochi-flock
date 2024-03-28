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
    margo_registered_name(mid, "flock_get_view", &id, &flag);

    if(flag == HG_TRUE) {
        margo_registered_name(mid, "flock_get_view", &c->get_view_id, &flag);
    } else {
        c->get_view_id = MARGO_REGISTER(mid, "flock_get_view", void, get_view_out_t, NULL);
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

struct flock_request {
    margo_request        request;
    hg_handle_t          rpc_handle;
    flock_group_handle_t group_handle;
    flock_return_t (*on_completion)(flock_request_t req);
};

static flock_return_t flock_group_update_view_cb(flock_request_t req)
{
    get_view_out_t out;
    hg_return_t hret;
    flock_return_t ret;

    hret = margo_get_output(req->rpc_handle, &out);
    if(hret != HG_SUCCESS) {
        ret = FLOCK_ERR_FROM_MERCURY;
        goto finish;
    }
    ret = out.ret;

    fprintf(stderr, "Received a group view with %lu members and %lu metadata\n",
            out.view.members.size, out.view.metadata.size);

    FLOCK_GROUP_VIEW_LOCK(&req->group_handle->view);
    flock_group_view_clear(&req->group_handle->view);
    FLOCK_GROUP_VIEW_MOVE(&out.view, &req->group_handle->view);
    FLOCK_GROUP_VIEW_UNLOCK(&req->group_handle->view);

    margo_free_output(req->rpc_handle, &out);

finish:
    margo_destroy(req->rpc_handle);
    free(req);
    return ret;
}

flock_return_t flock_group_update_view(
        flock_group_handle_t handle,
        flock_request_t* req)
{
    hg_handle_t   h;
    hg_return_t hret;

    hret = margo_create(handle->client->mid, handle->addr, handle->client->get_view_id, &h);
    if(hret != HG_SUCCESS)
        return FLOCK_ERR_FROM_MERCURY;

    flock_request_t tmp_req = (flock_request_t)calloc(1, sizeof(*tmp_req));
    tmp_req->rpc_handle = h;
    tmp_req->group_handle = handle;
    tmp_req->on_completion = flock_group_update_view_cb;

    hret = margo_provider_iforward(handle->provider_id, h, NULL, &tmp_req->request);
    if(hret != HG_SUCCESS) {
        margo_destroy(h);
        return FLOCK_ERR_FROM_MERCURY;
    }

    if(req) {
        *req = tmp_req;
        return FLOCK_SUCCESS;
    } else {
        return flock_request_wait(tmp_req);
    }
}

flock_return_t flock_group_subscribe(
        flock_group_handle_t handle,
        flock_membership_update_fn member_update_fn,
        flock_metadata_update_fn metadata_update_fn,
        void* context)
{
    (void)handle;
    (void)member_update_fn;
    (void)metadata_update_fn;
    (void)context;
    return FLOCK_ERR_OP_UNSUPPORTED;
}

flock_return_t flock_group_unsubscribe(
        flock_group_handle_t handle)
{
    (void)handle;
    return FLOCK_ERR_OP_UNSUPPORTED;
}

flock_return_t flock_request_wait(flock_request_t req)
{
    hg_return_t hret = margo_wait(req->request);
    if(hret != HG_SUCCESS) return FLOCK_ERR_FROM_MERCURY;
    return (req->on_completion)(req);
}

flock_return_t flock_request_test(flock_request_t req, bool* completed)
{
    int flag;
    hg_return_t hret = margo_test(req->request, &flag);
    if(hret != HG_SUCCESS) return FLOCK_ERR_FROM_MERCURY;
    *completed = flag;
    return FLOCK_SUCCESS;
}
