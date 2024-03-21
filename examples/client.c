/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdio.h>
#include <stdlib.h>
#include <margo.h>
#include <assert.h>
#include <alpha/alpha-client.h>
#include <alpha/alpha-resource.h>

#define FATAL(...) \
    do { \
        margo_critical(__VA_ARGS__); \
        exit(-1); \
    } while(0)

int main(int argc, char** argv)
{
    if(argc != 3) {
        fprintf(stderr,"Usage: %s <server address> <provider id>\n", argv[0]);
        exit(-1);
    }

    alpha_return_t ret;
    hg_return_t hret;
    const char* svr_addr_str = argv[1];
    uint16_t    provider_id  = atoi(argv[2]);

    margo_instance_id mid = margo_init("na+sm", MARGO_CLIENT_MODE, 0, 0);
    assert(mid);

    margo_set_log_level(mid, MARGO_LOG_INFO);

    hg_addr_t svr_addr;
    hret = margo_addr_lookup(mid, svr_addr_str, &svr_addr);
    if(hret != HG_SUCCESS) {
        FATAL(mid,"margo_addr_lookup failed for address %s", svr_addr_str);
    }

    alpha_client_t alpha_clt;
    alpha_resource_handle_t alpha_rh;

    margo_info(mid, "Creating ALPHA client");
    ret = alpha_client_init(mid, &alpha_clt);
    if(ret != ALPHA_SUCCESS) {
        FATAL(mid,"alpha_client_init failed (ret = %d)", ret);
    }

    margo_info(mid, "Creating resource handle for provider id %d", (int)provider_id);
    ret = alpha_resource_handle_create(alpha_clt, svr_addr, provider_id, true, &alpha_rh);
    if(ret != ALPHA_SUCCESS) {
        FATAL(mid,"alpha_resource_handle_create failed (ret = %d)", ret);
    }

    margo_info(mid, "Computing sum");
    int32_t result;
    ret = alpha_compute_sum(alpha_rh, 45, 23, &result);
    if(ret != ALPHA_SUCCESS) {
        FATAL(mid,"alpha_compute_sum failed (ret = %d)", ret);
    }
    margo_info(mid, "45 + 23 = %d", result);

    margo_info(mid, "Releasing resource handle");
    ret = alpha_resource_handle_release(alpha_rh);
    if(ret != ALPHA_SUCCESS) {
        FATAL(mid,"alpha_resource_handle_release failed (ret = %d)", ret);
    }

    margo_info(mid, "Finalizing client");
    ret = alpha_client_finalize(alpha_clt);
    if(ret != ALPHA_SUCCESS) {
        FATAL(mid,"alpha_client_finalize failed (ret = %d)", ret);
    }

    hret = margo_addr_free(mid, svr_addr);
    if(hret != HG_SUCCESS) {
        FATAL(mid,"Could not free address (margo_addr_free returned %d)", hret);
    }

    margo_finalize(mid);

    return 0;
}
