/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "types.h"
#include "client.h"
#include "alpha/alpha-client.h"

alpha_return_t alpha_client_init(margo_instance_id mid, alpha_client_t* client)
{
    alpha_client_t c = (alpha_client_t)calloc(1, sizeof(*c));
    if(!c) return ALPHA_ERR_ALLOCATION;

    c->mid = mid;

    hg_bool_t flag;
    hg_id_t id;
    margo_registered_name(mid, "alpha_sum", &id, &flag);

    if(flag == HG_TRUE) {
        margo_registered_name(mid, "alpha_sum", &c->sum_id, &flag);
    } else {
        c->sum_id = MARGO_REGISTER(mid, "alpha_sum", sum_in_t, sum_out_t, NULL);
    }

    *client = c;
    return ALPHA_SUCCESS;
}

alpha_return_t alpha_client_finalize(alpha_client_t client)
{
    if(client->num_resource_handles != 0) {
        margo_warning(client->mid,
            "%ld resource handles not released when alpha_client_finalize was called",
            client->num_resource_handles);
    }
    free(client);
    return ALPHA_SUCCESS;
}

alpha_return_t alpha_resource_handle_create(
        alpha_client_t client,
        hg_addr_t addr,
        uint16_t provider_id,
        bool check,
        alpha_resource_handle_t* handle)
{
    if(client == ALPHA_CLIENT_NULL)
        return ALPHA_ERR_INVALID_ARGS;

    hg_return_t ret;

    if(check) {
        char buffer[sizeof("alpha")];
        size_t bufsize = sizeof("alpha");
        ret = margo_provider_get_identity(client->mid, addr, provider_id, buffer, &bufsize);
        if(ret != HG_SUCCESS || strcmp("alpha", buffer) != 0)
            return ALPHA_ERR_INVALID_PROVIDER;
    }

    alpha_resource_handle_t rh =
        (alpha_resource_handle_t)calloc(1, sizeof(*rh));

    if(!rh) return ALPHA_ERR_ALLOCATION;

    ret = margo_addr_dup(client->mid, addr, &(rh->addr));
    if(ret != HG_SUCCESS) {
        free(rh);
        return ALPHA_ERR_FROM_MERCURY;
    }

    rh->client      = client;
    rh->provider_id = provider_id;
    rh->refcount    = 1;

    client->num_resource_handles += 1;

    *handle = rh;
    return ALPHA_SUCCESS;
}

alpha_return_t alpha_resource_handle_ref_incr(
        alpha_resource_handle_t handle)
{
    if(handle == ALPHA_RESOURCE_HANDLE_NULL)
        return ALPHA_ERR_INVALID_ARGS;
    handle->refcount += 1;
    return ALPHA_SUCCESS;
}

alpha_return_t alpha_resource_handle_release(alpha_resource_handle_t handle)
{
    if(handle == ALPHA_RESOURCE_HANDLE_NULL)
        return ALPHA_ERR_INVALID_ARGS;
    handle->refcount -= 1;
    if(handle->refcount == 0) {
        margo_addr_free(handle->client->mid, handle->addr);
        handle->client->num_resource_handles -= 1;
        free(handle);
    }
    return ALPHA_SUCCESS;
}

alpha_return_t alpha_compute_sum(
        alpha_resource_handle_t handle,
        int32_t x,
        int32_t y,
        int32_t* result)
{
    hg_handle_t   h;
    sum_in_t     in;
    sum_out_t   out;
    hg_return_t hret;
    alpha_return_t ret;

    in.x = x;
    in.y = y;

    hret = margo_create(handle->client->mid, handle->addr, handle->client->sum_id, &h);
    if(hret != HG_SUCCESS)
        return ALPHA_ERR_FROM_MERCURY;

    hret = margo_provider_forward(handle->provider_id, h, &in);
    if(hret != HG_SUCCESS) {
        ret = ALPHA_ERR_FROM_MERCURY;
        goto finish;
    }

    hret = margo_get_output(h, &out);
    if(hret != HG_SUCCESS) {
        ret = ALPHA_ERR_FROM_MERCURY;
        goto finish;
    }

    ret = out.ret;
    if(ret == ALPHA_SUCCESS)
        *result = out.result;

    margo_free_output(h, &out);

finish:
    margo_destroy(h);
    return ret;
}
