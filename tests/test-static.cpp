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
#include <unordered_map>

struct TestContext {

    margo_instance_id mid = MARGO_INSTANCE_NULL;
    hg_addr_t         addr = HG_ADDR_NULL;

    TestContext() {
        mid = margo_init("na+sm", MARGO_SERVER_MODE, 0, 0);
        margo_set_log_level(mid, MARGO_LOG_TRACE);
        margo_addr_self(mid, &addr);
    }

    ~TestContext() {
        margo_addr_free(mid, addr);
        margo_finalize(mid);
    }
};

TEST_CASE("Test group handle for centralized group", "[centralize]") {

    // create test context
    auto context = std::make_unique<TestContext>();

    char self_addr[256];
    hg_size_t self_addr_size = 256;
    margo_addr_to_string(context->mid, self_addr, &self_addr_size, context->addr);

    std::stringstream ss;
    ss << R"({
            "group":{
                "type":"static",
                "config":{}
            },
            "bootstrap": "view"
           })";

    // create test group with 5 processes
    auto config = ss.str();
    auto group = std::make_unique<TestGroup>(context->mid, 5, config.c_str());

    SECTION("Test provider functionalities") {
        char* config = flock_provider_get_config(group->providers[0]);
        REQUIRE(config != nullptr);
        const char* expected = R"({"group":{"type":"static"},"config":{}})";
        REQUIRE(strcmp(config, expected) == 0);
        free(config);
    }

    SECTION("Create client and group handle") {
        flock_client_t client;
        flock_return_t ret;
        // create a client object
        ret = flock_client_init(context->mid, ABT_POOL_NULL, &client);
        REQUIRE(ret == FLOCK_SUCCESS);

        flock_group_handle_t rh;
        // create a group handle
        ret = flock_group_handle_create(client,
                context->addr, 1, FLOCK_MODE_INIT_UPDATE, &rh);
        REQUIRE(ret == FLOCK_SUCCESS);

        // get view
        flock_group_view_t view = FLOCK_GROUP_VIEW_INITIALIZER;
        ret = flock_group_get_view(rh, &view);
        REQUIRE(ret == FLOCK_SUCCESS);

        // test group size
        auto group_size = flock_group_view_member_count(&view);
        REQUIRE(ret == FLOCK_SUCCESS);
        REQUIRE(group_size == 5);

        // test iterate over members
        for(size_t i = 0; i < group_size; ++i) {
            flock_member_t* member = flock_group_view_member_at(&view, i);
            REQUIRE(member != nullptr);
            REQUIRE(member->provider_id == i+1);
            REQUIRE(strcmp(member->address, self_addr) == 0);
        }

        // test metadata count
        auto metadata_count = flock_group_view_metadata_count(&view);
        REQUIRE(ret == FLOCK_SUCCESS);
        REQUIRE(metadata_count == 4); // account for __config__ and __backend__ keys

        // test iterate over members
        for(size_t i = 0; i < metadata_count; ++i) {
            flock_metadata_t* metadata = flock_group_view_metadata_at(&view, i);
            REQUIRE(metadata != nullptr);
            REQUIRE(metadata->key != nullptr);
            REQUIRE(metadata->value != nullptr);
        }

        // test finding metadata
        REQUIRE(strcmp(flock_group_view_find_metadata(&view, "matthieu"), "dorier") == 0);
        REQUIRE(strcmp(flock_group_view_find_metadata(&view, "shane"), "snyder") == 0);
        REQUIRE(flock_group_view_find_metadata(&view, "abcd") == nullptr);

        // destroy the view
        flock_group_view_clear(&view);

        // destroy the group handle
        ret = flock_group_handle_release(rh);
        REQUIRE(ret == FLOCK_SUCCESS);

        // test that we can free the client object
        ret = flock_client_finalize(client);
        REQUIRE(ret == FLOCK_SUCCESS);
    }

}
