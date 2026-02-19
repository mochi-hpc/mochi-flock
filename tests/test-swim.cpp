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
#include "../src/backends/swim/swim-backend.h"
#include "helper.hpp"
#include <unordered_map>
#include <atomic>

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

TEST_CASE("Test SWIM backend basic initialization", "[swim]") {

    auto context = std::make_unique<TestContext>();

    char self_addr[256];
    hg_size_t self_addr_size = 256;
    margo_addr_to_string(context->mid, self_addr, &self_addr_size, context->addr);

    std::stringstream ss;
    ss << R"({
            "group":{
                "type":"swim",
                "config":{
                    "protocol_period_ms": 500.0,
                    "ping_timeout_ms": 100.0,
                    "ping_req_timeout_ms": 200.0,
                    "ping_req_members": 2,
                    "suspicion_timeout_ms": 2000.0
                }
            },
            "bootstrap": "view"
           })";

    auto config = ss.str();
    auto group = std::make_unique<TestGroup>(context->mid, 3, config.c_str());

    SECTION("Test provider configuration") {
        char* config = flock_provider_get_config(group->providers[0]);
        REQUIRE(config != nullptr);
        REQUIRE(strstr(config, "swim") != nullptr);
        free(config);
    }

    SECTION("Test initial group view") {
        flock_client_t client;
        flock_return_t ret;

        ret = flock_client_init(context->mid, ABT_POOL_NULL, &client);
        REQUIRE(ret == FLOCK_SUCCESS);

        flock_group_handle_t rh;
        ret = flock_group_handle_create(client, context->addr, 1, FLOCK_MODE_INIT_UPDATE, &rh);
        REQUIRE(ret == FLOCK_SUCCESS);

        flock_group_view_t view = FLOCK_GROUP_VIEW_INITIALIZER;
        ret = flock_group_get_view(rh, &view);
        REQUIRE(ret == FLOCK_SUCCESS);

        auto group_size = flock_group_view_member_count(&view);
        REQUIRE(group_size == 3);

        for(size_t i = 0; i < group_size; ++i) {
            flock_member_t* member = flock_group_view_member_at(&view, i);
            REQUIRE(member != nullptr);
            REQUIRE(strcmp(member->address, self_addr) == 0);
        }

        flock_group_view_clear(&view);
        flock_group_handle_release(rh);
        flock_client_finalize(client);
    }

    SECTION("Test SWIM ping protocol") {
        // Let the SWIM protocol run for a few cycles
        margo_thread_sleep(context->mid, 3000);

        // All members should still be alive after the pings
        flock_client_t client;
        flock_return_t ret = flock_client_init(context->mid, ABT_POOL_NULL, &client);
        REQUIRE(ret == FLOCK_SUCCESS);

        flock_group_handle_t rh;
        ret = flock_group_handle_create(client, context->addr, 1, FLOCK_MODE_INIT_UPDATE, &rh);
        REQUIRE(ret == FLOCK_SUCCESS);

        flock_group_view_t view = FLOCK_GROUP_VIEW_INITIALIZER;
        ret = flock_group_get_view(rh, &view);
        REQUIRE(ret == FLOCK_SUCCESS);

        auto group_size = flock_group_view_member_count(&view);
        REQUIRE(group_size == 3);

        flock_group_view_clear(&view);
        flock_group_handle_release(rh);
        flock_client_finalize(client);
    }
}

TEST_CASE("Test SWIM member join", "[swim]") {

    auto context = std::make_unique<TestContext>();

    char self_addr[256];
    hg_size_t self_addr_size = 256;
    margo_addr_to_string(context->mid, self_addr, &self_addr_size, context->addr);

    std::stringstream ss;
    ss << R"({
            "group":{
                "type":"swim",
                "config":{
                    "protocol_period_ms": 500.0,
                    "ping_timeout_ms": 100.0,
                    "ping_req_timeout_ms": 200.0,
                    "ping_req_members": 2,
                    "suspicion_timeout_ms": 2000.0
                }
            },
            "bootstrap": "view"
           })";

    auto config = ss.str();

    // Create initial group with 2 providers (provider IDs 1 and 2)
    auto group = std::make_unique<TestGroup>(context->mid, 2, config.c_str());

    SECTION("Third member joins existing group") {
        // Let the group stabilize
        margo_thread_sleep(context->mid, 1000);

        // Register a 3rd provider whose initial view contains only
        // provider 1. Because provider 3 is not in its own initial
        // view, flock_provider_register sets join=true, which triggers
        // the SWIM backend's gossip-based join announcement.
        flock_group_view_t join_view = FLOCK_GROUP_VIEW_INITIALIZER;
        flock_group_view_add_member(&join_view, self_addr, 1);

        struct flock_provider_args join_args = FLOCK_PROVIDER_ARGS_INIT;
        join_args.initial_view = &join_view;

        flock_provider_t joining_provider = nullptr;
        flock_return_t ret = flock_provider_register(
            context->mid, 3, config.c_str(), &join_args, &joining_provider);
        REQUIRE(ret == FLOCK_SUCCESS);

        // Add to group so it gets cleaned up by the TestGroup destructor
        group->providers.push_back(joining_provider);

        // Allow time for gossip to propagate the join to all members
        margo_thread_sleep(context->mid, 3000);

        // Query provider 1's view to verify the 3rd member was added
        flock_client_t client;
        ret = flock_client_init(context->mid, ABT_POOL_NULL, &client);
        REQUIRE(ret == FLOCK_SUCCESS);

        flock_group_handle_t rh;
        ret = flock_group_handle_create(client,
                context->addr, 1, FLOCK_MODE_INIT_UPDATE, &rh);
        REQUIRE(ret == FLOCK_SUCCESS);

        flock_group_view_t view = FLOCK_GROUP_VIEW_INITIALIZER;
        ret = flock_group_get_view(rh, &view);
        REQUIRE(ret == FLOCK_SUCCESS);

        // Should now have 3 members
        REQUIRE(flock_group_view_member_count(&view) == 3);

        // Verify all three members are present with correct provider IDs
        for(size_t i = 0; i < 3; ++i) {
            flock_member_t* member = flock_group_view_member_at(&view, i);
            REQUIRE(member != nullptr);
            REQUIRE(member->provider_id == i + 1);
            REQUIRE(strcmp(member->address, self_addr) == 0);
        }

        flock_group_view_clear(&view);

        // Let the SWIM protocol run a few more cycles to verify
        // stability after the view array was reallocated during join
        margo_thread_sleep(context->mid, 3000);

        ret = flock_group_handle_release(rh);
        REQUIRE(ret == FLOCK_SUCCESS);

        ret = flock_client_finalize(client);
        REQUIRE(ret == FLOCK_SUCCESS);
    }
}

TEST_CASE("Test SWIM failure detection", "[swim]") {

    auto context = std::make_unique<TestContext>();

    char self_addr[256];
    hg_size_t self_addr_size = 256;
    margo_addr_to_string(context->mid, self_addr, &self_addr_size, context->addr);

    std::stringstream ss;
    ss << R"({
            "group":{
                "type":"swim",
                "config":{
                    "protocol_period_ms": 300.0,
                    "ping_timeout_ms": 100.0,
                    "ping_req_timeout_ms": 200.0,
                    "ping_req_members": 2,
                    "suspicion_timeout_ms": 1500.0
                }
            },
            "bootstrap": "view"
           })";

    auto config = ss.str();
    auto group = std::make_unique<TestGroup>(context->mid, 5, config.c_str());

    SECTION("Detect member failure") {
        // Let the group stabilize
        margo_thread_sleep(context->mid, 1000);

        // Get initial view
        flock_client_t client;
        flock_return_t ret = flock_client_init(context->mid, ABT_POOL_NULL, &client);
        REQUIRE(ret == FLOCK_SUCCESS);

        flock_group_handle_t rh;
        ret = flock_group_handle_create(client, context->addr, 1, FLOCK_MODE_INIT_UPDATE, &rh);
        REQUIRE(ret == FLOCK_SUCCESS);

        flock_group_view_t view = FLOCK_GROUP_VIEW_INITIALIZER;
        ret = flock_group_get_view(rh, &view);
        REQUIRE(ret == FLOCK_SUCCESS);
        REQUIRE(flock_group_view_member_count(&view) == 5);
        flock_group_view_clear(&view);

        // Simulate crash: enable crash mode so no LEAVE announcement is sent
        // This forces failure detection via failed pings and suspicion timeout
        ret = flock_swim_set_crash_mode(group->providers.back(), true);
        REQUIRE(ret == FLOCK_SUCCESS);

        // Kill one provider (the last one) - no leave announcement will be sent
        flock_provider_destroy(group->providers.back());
        group->providers.pop_back();

        // Wait for failure detection (suspicion timeout + some protocol periods)
        // This must go through the full suspicion timeout since there's no LEAVE
        margo_thread_sleep(context->mid, 5000);

        // Update view
        ret = flock_group_update_view(rh, NULL);
        REQUIRE(ret == FLOCK_SUCCESS);

        ret = flock_group_get_view(rh, &view);
        REQUIRE(ret == FLOCK_SUCCESS);

        auto group_size = flock_group_view_member_count(&view);
        REQUIRE(group_size == 4);

        flock_group_view_clear(&view);
        flock_group_handle_release(rh);
        flock_client_finalize(client);
    }
}

TEST_CASE("Test SWIM graceful leave", "[swim]") {

    auto context = std::make_unique<TestContext>();

    char self_addr[256];
    hg_size_t self_addr_size = 256;
    margo_addr_to_string(context->mid, self_addr, &self_addr_size, context->addr);

    std::stringstream ss;
    ss << R"({
            "group":{
                "type":"swim",
                "config":{
                    "protocol_period_ms": 300.0,
                    "ping_timeout_ms": 100.0,
                    "ping_req_timeout_ms": 200.0,
                    "ping_req_members": 2,
                    "suspicion_timeout_ms": 2000.0
                }
            },
            "bootstrap": "view"
           })";

    auto config = ss.str();
    auto group = std::make_unique<TestGroup>(context->mid, 4, config.c_str());

    SECTION("Member gracefully leaves") {
        // Let the group stabilize
        margo_thread_sleep(context->mid, 1000);

        flock_client_t client;
        flock_return_t ret = flock_client_init(context->mid, ABT_POOL_NULL, &client);
        REQUIRE(ret == FLOCK_SUCCESS);

        flock_group_handle_t rh;
        ret = flock_group_handle_create(client, context->addr, 1, FLOCK_MODE_INIT_UPDATE, &rh);
        REQUIRE(ret == FLOCK_SUCCESS);

        flock_group_view_t view = FLOCK_GROUP_VIEW_INITIALIZER;
        ret = flock_group_get_view(rh, &view);
        REQUIRE(ret == FLOCK_SUCCESS);
        REQUIRE(flock_group_view_member_count(&view) == 4);
        flock_group_view_clear(&view);

        // Gracefully destroy one provider - sends LEAVE announcement
        // (crash mode is NOT enabled, so this is a graceful leave)
        flock_provider_destroy(group->providers.back());
        group->providers.pop_back();

        // Wait for the leave to propagate - should be quick since LEAVE is announced directly
        // Much shorter than failure detection which requires suspicion timeout
        margo_thread_sleep(context->mid, 1000);

        // Update and check view
        ret = flock_group_update_view(rh, NULL);
        REQUIRE(ret == FLOCK_SUCCESS);

        ret = flock_group_get_view(rh, &view);
        REQUIRE(ret == FLOCK_SUCCESS);
        REQUIRE(flock_group_view_member_count(&view) == 3);

        flock_group_view_clear(&view);
        flock_group_handle_release(rh);
        flock_client_finalize(client);
    }
}

TEST_CASE("Test SWIM with single member", "[swim]") {

    auto context = std::make_unique<TestContext>();

    std::stringstream ss;
    ss << R"({
            "group":{
                "type":"swim",
                "config":{
                    "protocol_period_ms": 500.0,
                    "ping_timeout_ms": 100.0,
                    "ping_req_timeout_ms": 200.0,
                    "ping_req_members": 2,
                    "suspicion_timeout_ms": 2000.0
                }
            },
            "bootstrap": "view"
           })";

    auto config = ss.str();

    SECTION("Single member group works") {
        auto group = std::make_unique<TestGroup>(context->mid, 1, config.c_str());

        // Let it run for a bit
        margo_thread_sleep(context->mid, 2000);

        flock_client_t client;
        flock_return_t ret = flock_client_init(context->mid, ABT_POOL_NULL, &client);
        REQUIRE(ret == FLOCK_SUCCESS);

        flock_group_handle_t rh;
        ret = flock_group_handle_create(client, context->addr, 1, FLOCK_MODE_INIT_UPDATE, &rh);
        REQUIRE(ret == FLOCK_SUCCESS);

        flock_group_view_t view = FLOCK_GROUP_VIEW_INITIALIZER;
        ret = flock_group_get_view(rh, &view);
        REQUIRE(ret == FLOCK_SUCCESS);
        REQUIRE(flock_group_view_member_count(&view) == 1);

        flock_group_view_clear(&view);
        flock_group_handle_release(rh);
        flock_client_finalize(client);
    }
}

TEST_CASE("Test SWIM metadata", "[swim]") {

    auto context = std::make_unique<TestContext>();

    std::stringstream ss;
    ss << R"({
            "group":{
                "type":"swim",
                "config":{}
            },
            "bootstrap": "view"
           })";

    auto config = ss.str();
    auto group = std::make_unique<TestGroup>(context->mid, 2, config.c_str());

    SECTION("Metadata is preserved") {
        flock_client_t client;
        flock_return_t ret = flock_client_init(context->mid, ABT_POOL_NULL, &client);
        REQUIRE(ret == FLOCK_SUCCESS);

        flock_group_handle_t rh;
        ret = flock_group_handle_create(client, context->addr, 1, FLOCK_MODE_INIT_UPDATE, &rh);
        REQUIRE(ret == FLOCK_SUCCESS);

        flock_group_view_t view = FLOCK_GROUP_VIEW_INITIALIZER;
        ret = flock_group_get_view(rh, &view);
        REQUIRE(ret == FLOCK_SUCCESS);

        // Check that metadata from TestGroup is preserved
        REQUIRE(flock_group_view_find_metadata(&view, "matthieu") != nullptr);
        REQUIRE(strcmp(flock_group_view_find_metadata(&view, "matthieu"), "dorier") == 0);
        REQUIRE(flock_group_view_find_metadata(&view, "shane") != nullptr);
        REQUIRE(strcmp(flock_group_view_find_metadata(&view, "shane"), "snyder") == 0);

        // Check SWIM-specific metadata
        REQUIRE(flock_group_view_find_metadata(&view, "__type__") != nullptr);
        REQUIRE(strcmp(flock_group_view_find_metadata(&view, "__type__"), "swim") == 0);

        flock_group_view_clear(&view);
        flock_group_handle_release(rh);
        flock_client_finalize(client);
    }
}
