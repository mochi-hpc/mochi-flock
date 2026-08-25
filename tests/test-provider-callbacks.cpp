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
#include "helper.hpp"

/* Callbacks used by the tests. They only record that they were invoked. */
static void member_cb(void* ctx, flock_update_t, const char*, uint16_t)
{
    if(ctx) (*static_cast<int*>(ctx))++;
}

static void metadata_cb(void* ctx, const char*, const char*)
{
    if(ctx) (*static_cast<int*>(ctx))++;
}

struct TestContext {

    margo_instance_id mid = MARGO_INSTANCE_NULL;

    TestContext() {
        mid = margo_init("na+sm", MARGO_SERVER_MODE, 0, 0);
    }

    ~TestContext() {
        margo_finalize(mid);
    }
};

TEST_CASE("Test provider update callbacks", "[provider-callbacks]") {

    auto context = std::make_unique<TestContext>();
    auto group   = std::make_unique<TestGroup>(context->mid, 1);
    flock_provider_t provider = group->providers[0];

    int ctx_a = 0, ctx_b = 0;

    SECTION("Adding a callback to a freshly registered provider") {
        // Regression for issue #7: update_callbacks is initialized to NULL,
        // and the first add used to dereference that NULL head.
        flock_return_t ret = flock_provider_add_update_callbacks(
                provider, member_cb, metadata_cb, &ctx_a);
        REQUIRE(ret == FLOCK_SUCCESS);
    }

    SECTION("Adding with the same context replaces, different context appends") {
        REQUIRE(flock_provider_add_update_callbacks(
                    provider, member_cb, metadata_cb, &ctx_a) == FLOCK_SUCCESS);
        // same context again: must succeed (replace), not append a broken node
        REQUIRE(flock_provider_add_update_callbacks(
                    provider, member_cb, metadata_cb, &ctx_a) == FLOCK_SUCCESS);
        // a distinct context: appended at the tail
        REQUIRE(flock_provider_add_update_callbacks(
                    provider, member_cb, metadata_cb, &ctx_b) == FLOCK_SUCCESS);
    }

    SECTION("Registering only one of the two callback types is allowed") {
        // Only a membership callback (NULL metadata callback).
        REQUIRE(flock_provider_add_update_callbacks(
                    provider, member_cb, nullptr, &ctx_a) == FLOCK_SUCCESS);
        // Only a metadata callback (NULL membership callback).
        REQUIRE(flock_provider_add_update_callbacks(
                    provider, nullptr, metadata_cb, &ctx_b) == FLOCK_SUCCESS);
    }

    SECTION("Removing from an empty list does not crash") {
        // Regression for issue #7: remove used to dereference the NULL head.
        flock_return_t ret = flock_provider_remove_update_callbacks(
                provider, &ctx_a);
        REQUIRE(ret == FLOCK_ERR_INVALID_ARGS);
    }

    SECTION("Removing an unregistered context does not run off the tail") {
        REQUIRE(flock_provider_add_update_callbacks(
                    provider, member_cb, metadata_cb, &ctx_a) == FLOCK_SUCCESS);
        // ctx_b was never registered: must report an error, not crash.
        REQUIRE(flock_provider_remove_update_callbacks(
                    provider, &ctx_b) == FLOCK_ERR_INVALID_ARGS);
    }

    SECTION("Add then remove a registered context succeeds") {
        REQUIRE(flock_provider_add_update_callbacks(
                    provider, member_cb, metadata_cb, &ctx_a) == FLOCK_SUCCESS);
        REQUIRE(flock_provider_remove_update_callbacks(
                    provider, &ctx_a) == FLOCK_SUCCESS);
        // removing it a second time now reports an error
        REQUIRE(flock_provider_remove_update_callbacks(
                    provider, &ctx_a) == FLOCK_ERR_INVALID_ARGS);
    }
}
