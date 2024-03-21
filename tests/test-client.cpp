/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdio.h>
#include <margo.h>
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <flock/flock-server.h>
#include <flock/flock-client.h>
#include <flock/flock-group.h>

struct test_context {
    margo_instance_id mid;
    hg_addr_t         addr;
};

static const uint16_t provider_id = 42;
static const char* provider_config = "{ \"group\":{ \"type\":\"dummy\", \"config\":{} } }";

TEST_CASE("Test client interface", "[client]") {

    flock_return_t      ret;
    margo_instance_id   mid;
    hg_addr_t           addr;
    // create margo instance
    mid = margo_init("na+sm", MARGO_SERVER_MODE, 0, 0);
    REQUIRE(mid != MARGO_INSTANCE_NULL);
    // get address of current process
    hg_return_t hret = margo_addr_self(mid, &addr);
    REQUIRE(hret == HG_SUCCESS);
    // register flock provider
    struct flock_provider_args args = FLOCK_PROVIDER_ARGS_INIT;
    ret = flock_provider_register(
            mid, provider_id, provider_config, &args,
            FLOCK_PROVIDER_IGNORE);
    REQUIRE(ret == FLOCK_SUCCESS);
    // create test context
    auto context = std::make_unique<test_context>();
    context->mid   = mid;
    context->addr  = addr;

    SECTION("Create client") {
        flock_client_t client;
        flock_return_t ret;
        // test that we can create a client object
        ret = flock_client_init(context->mid, &client);
        REQUIRE(ret == FLOCK_SUCCESS);

        SECTION("Open group") {
            flock_group_handle_t rh;
            // test that we can create a group handle
            ret = flock_group_handle_create(client,
                    context->addr, provider_id, true, &rh);
            REQUIRE(ret == FLOCK_SUCCESS);

            // test that we get an error when using a wrong provider ID
            flock_group_handle_t rh2;
            ret = flock_group_handle_create(client,
                      context->addr, provider_id + 123, true, &rh2);
            REQUIRE(ret == FLOCK_ERR_INVALID_PROVIDER);

            SECTION("Send sum RPC") {
                // test that we can send a sum RPC to the group
                int32_t result = 0;
                ret = flock_compute_sum(rh, 45, 55, &result);
                REQUIRE(ret == FLOCK_SUCCESS);
                REQUIRE(result == 100);
            }

            // test that we can increase the ref count
            ret = flock_group_handle_ref_incr(rh);
            REQUIRE(ret == FLOCK_SUCCESS);
            // test that we can destroy the group handle
            ret = flock_group_handle_release(rh);
            REQUIRE(ret == FLOCK_SUCCESS);
            // ... and a second time because of the increase ref
            ret = flock_group_handle_release(rh);
            REQUIRE(ret == FLOCK_SUCCESS);
        }

        // test that we can free the client object
        ret = flock_client_finalize(client);
        REQUIRE(ret == FLOCK_SUCCESS);
    }

    // free address
    margo_addr_free(context->mid, context->addr);
    // we are not checking the return value of the above function with
    // munit because we need margo_finalize to be called no matter what.
    margo_finalize(context->mid);
}
