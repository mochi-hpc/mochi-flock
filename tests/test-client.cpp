/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdio.h>
#include <margo.h>
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <alpha/alpha-server.h>
#include <alpha/alpha-client.h>
#include <alpha/alpha-resource.h>

struct test_context {
    margo_instance_id mid;
    hg_addr_t         addr;
};

static const uint16_t provider_id = 42;
static const char* provider_config = "{ \"resource\":{ \"type\":\"dummy\", \"config\":{} } }";

TEST_CASE("Test client interface", "[client]") {

    alpha_return_t      ret;
    margo_instance_id   mid;
    hg_addr_t           addr;
    // create margo instance
    mid = margo_init("na+sm", MARGO_SERVER_MODE, 0, 0);
    REQUIRE(mid != MARGO_INSTANCE_NULL);
    // get address of current process
    hg_return_t hret = margo_addr_self(mid, &addr);
    REQUIRE(hret == HG_SUCCESS);
    // register alpha provider
    struct alpha_provider_args args = ALPHA_PROVIDER_ARGS_INIT;
    ret = alpha_provider_register(
            mid, provider_id, provider_config, &args,
            ALPHA_PROVIDER_IGNORE);
    REQUIRE(ret == ALPHA_SUCCESS);
    // create test context
    auto context = std::make_unique<test_context>();
    context->mid   = mid;
    context->addr  = addr;

    SECTION("Create client") {
        alpha_client_t client;
        alpha_return_t ret;
        // test that we can create a client object
        ret = alpha_client_init(context->mid, &client);
        REQUIRE(ret == ALPHA_SUCCESS);

        SECTION("Open resource") {
            alpha_resource_handle_t rh;
            // test that we can create a resource handle
            ret = alpha_resource_handle_create(client,
                    context->addr, provider_id, true, &rh);
            REQUIRE(ret == ALPHA_SUCCESS);

            // test that we get an error when using a wrong provider ID
            alpha_resource_handle_t rh2;
            ret = alpha_resource_handle_create(client,
                      context->addr, provider_id + 123, true, &rh2);
            REQUIRE(ret == ALPHA_ERR_INVALID_PROVIDER);

            SECTION("Send sum RPC") {
                // test that we can send a sum RPC to the resource
                int32_t result = 0;
                ret = alpha_compute_sum(rh, 45, 55, &result);
                REQUIRE(ret == ALPHA_SUCCESS);
                REQUIRE(result == 100);
            }

            // test that we can increase the ref count
            ret = alpha_resource_handle_ref_incr(rh);
            REQUIRE(ret == ALPHA_SUCCESS);
            // test that we can destroy the resource handle
            ret = alpha_resource_handle_release(rh);
            REQUIRE(ret == ALPHA_SUCCESS);
            // ... and a second time because of the increase ref
            ret = alpha_resource_handle_release(rh);
            REQUIRE(ret == ALPHA_SUCCESS);
        }

        // test that we can free the client object
        ret = alpha_client_finalize(client);
        REQUIRE(ret == ALPHA_SUCCESS);
    }

    // free address
    margo_addr_free(context->mid, context->addr);
    // we are not checking the return value of the above function with
    // munit because we need margo_finalize to be called no matter what.
    margo_finalize(context->mid);
}
