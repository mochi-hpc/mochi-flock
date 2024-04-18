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

    // create test group with 5 processes
    auto group = std::make_unique<TestGroup>(context->mid, 5,
        R"({
            "group":{
                "type":"centralized",
                "config":{
                    "ping_timeout_ms": [800.0, 1000.0],
                    "ping_interval_ms": [800.0, 1000.0],
                    "ping_max_num_timeouts": 2
                }
            },
            "bootstrap": "view"
           })");

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

        // test flock_group_size
        size_t group_size = 0;
        ret = flock_group_size(rh, &group_size);
        REQUIRE(ret == FLOCK_SUCCESS);
        REQUIRE(group_size == 5);

        // test flock_group_live_member_count
        size_t count = 0;
        ret = flock_group_live_member_count(rh, &count);
        REQUIRE(ret == FLOCK_SUCCESS);
        REQUIRE(count == 5);

        // test iterate over members
        std::vector<std::tuple<size_t, std::string, uint16_t>> members;
        ret = flock_group_member_iterate(rh,
            [](void* u, size_t rank, const char* address, uint16_t provider_id) -> bool {
                auto members_found = static_cast<decltype(members)*>(u);
                members_found->push_back({rank, address, provider_id});
                return true;
            }, &members);
        REQUIRE(ret == FLOCK_SUCCESS);
        REQUIRE(members.size() == 5);
        for(size_t i = 0; i < members.size(); ++i) {
            REQUIRE(std::get<0>(members[i]) == i);
            REQUIRE(!std::get<1>(members[i]).empty());
            REQUIRE(std::get<2>(members[i]) == i+1);
        }

        // test getting addresses and provider IDs with correct ranks
        for(size_t i = 0; i < 5; ++i) {
            hg_addr_t addr = HG_ADDR_NULL;
            ret = flock_group_member_get_address(rh, i, &addr);
            REQUIRE(ret == FLOCK_SUCCESS);
            REQUIRE(addr != HG_ADDR_NULL);
            margo_addr_free(context->mid, addr);

            char* address = NULL;
            ret = flock_group_member_get_address_string(rh, i, &address);
            REQUIRE(ret == FLOCK_SUCCESS);
            REQUIRE(address != NULL);
            free(address);

            uint16_t provider_id = 0;
            ret = flock_group_member_get_provider_id(rh, i, &provider_id);
            REQUIRE(ret == FLOCK_SUCCESS);
            REQUIRE(provider_id == i+1);
        }

        // test getting addresses and provider ID with invalid rank
        hg_addr_t addr = HG_ADDR_NULL;
        ret = flock_group_member_get_address(rh, 5, &addr);
        REQUIRE(ret == FLOCK_ERR_NO_MEMBER);

        char* address = NULL;
        ret = flock_group_member_get_address_string(rh, 5, &address);
        REQUIRE(ret == FLOCK_ERR_NO_MEMBER);

        uint16_t provider_id = 0;
        ret = flock_group_member_get_provider_id(rh, 5, &provider_id);
        REQUIRE(ret == FLOCK_ERR_NO_MEMBER);

        // test getting rank from address and provider ID
        size_t rank = 0;
        ret = flock_group_member_get_rank(rh, std::get<1>(members[2]).c_str(), 3, &rank);
        REQUIRE(ret == FLOCK_SUCCESS);
        REQUIRE(rank == 2);

        // test getting rank from an invalid address
        ret = flock_group_member_get_rank(rh, "abcdefgh", 3, &rank);
        REQUIRE(ret == FLOCK_ERR_NO_MEMBER);

        // test getting rank from an invalid provider ID
        ret = flock_group_member_get_rank(rh, std::get<1>(members[2]).c_str(), 123, &rank);
        REQUIRE(ret == FLOCK_ERR_NO_MEMBER);

        // test iterate over metadata
        std::unordered_map<std::string, std::string> metadata;
        ret = flock_group_metadata_iterate(rh,
            [](void* u, const char* key, const char* value) -> bool {
                auto md = static_cast<decltype(metadata)*>(u);
                md->insert({key, value});
                return true;
            }, &metadata);
        REQUIRE(ret == FLOCK_SUCCESS);
        REQUIRE(metadata.size() == 2);
        REQUIRE(metadata.count("matthieu") == 1);
        REQUIRE(metadata["matthieu"] == "dorier");
        REQUIRE(metadata.count("shane") == 1);
        REQUIRE(metadata["shane"] == "snyder");

        // access metadata individually
        bool found = false;
        ret = flock_group_metadata_access(rh, "matthieu",
                [](void* u, const char*, const char* val) {
                    bool* b = static_cast<bool*>(u);
                    *b = val && std::string{val} == "dorier";
                    return true;
                }, &found);
        REQUIRE(ret == FLOCK_SUCCESS);
        REQUIRE(found);

        ret = flock_group_metadata_access(rh, "abcd",
                [](void*, const char*, const char*) { return true; }, nullptr);
        REQUIRE(ret == FLOCK_ERR_NO_METADATA);

        // destroy the group handle
        ret = flock_group_handle_release(rh);
        REQUIRE(ret == FLOCK_SUCCESS);

        // test that we can free the client object
        ret = flock_client_finalize(client);
        REQUIRE(ret == FLOCK_SUCCESS);

        margo_thread_sleep(context->mid, 5000);
    }
}
