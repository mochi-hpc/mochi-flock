/*
 * (C) 2024 The University of Chicago
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
#include "helper.hpp"

struct TestContext {

    margo_instance_id mid = MARGO_INSTANCE_NULL;
    hg_addr_t         addr = HG_ADDR_NULL;

    TestContext() {
        mid = margo_init("na+sm", MARGO_SERVER_MODE, 0, 0);
        margo_addr_self(mid, &addr);
    }

    ~TestContext() {
        margo_addr_free(mid, addr);
        margo_finalize(mid);
    }
};

TEST_CASE("Test client interface", "[client]") {

    // create test context
    auto context = std::make_unique<TestContext>();

    // create test group with 5 processes
    auto group = std::make_unique<TestGroup>(context->mid, 5);

    SECTION("Create client") {
        flock_client_t client;
        flock_return_t ret;
        // test that we can create a client object
        ret = flock_client_init(context->mid, ABT_POOL_NULL, &client);
        REQUIRE(ret == FLOCK_SUCCESS);

        SECTION("Create group handle") {
            flock_group_handle_t rh;
            // test that we can create a group handle
            ret = flock_group_handle_create(client,
                    context->addr, 1, 0, &rh);
            REQUIRE(ret == FLOCK_SUCCESS);

            // test that we get an error when using a wrong provider ID
            flock_group_handle_t rh2;
            ret = flock_group_handle_create(client,
                      context->addr, 123, 0, &rh2);
            REQUIRE(ret == FLOCK_ERR_INVALID_PROVIDER);

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
}
