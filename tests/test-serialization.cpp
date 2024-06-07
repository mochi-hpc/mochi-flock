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
        //margo_set_log_level(mid, MARGO_LOG_TRACE);
        margo_addr_self(mid, &addr);
    }

    ~TestContext() {
        margo_addr_free(mid, addr);
        margo_finalize(mid);
    }
};

TEST_CASE("Test group handle serialization and deserialization", "[serialization]") {

    // create test context
    auto context = std::make_unique<TestContext>();

    std::string filename = "tmp-group-file." + std::to_string(time(nullptr));

    auto provider_config = std::string{
        R"({
             "group":{
                "type":"static",
                "config":{}
           },
           "file":")"} + filename + R"("
           })";

    // create test group with 5 processes
    auto group = std::make_unique<TestGroup>(context->mid, 5, provider_config.c_str());

    char self_addr[256];
    hg_size_t self_addr_size = 256;
    margo_addr_to_string(context->mid, self_addr, &self_addr_size, context->addr);

    flock_client_t client;
    flock_return_t ret;
    // create a client object
    ret = flock_client_init(context->mid, ABT_POOL_NULL, &client);
    REQUIRE(ret == FLOCK_SUCCESS);

    flock_group_handle_t rh1;

    SECTION("Serialize and deserialize using group handle") {
        // create a group handle
        ret = flock_group_handle_create(client,
                context->addr, 1, FLOCK_MODE_INIT_UPDATE, &rh1);
        REQUIRE(ret == FLOCK_SUCCESS);

        SECTION("Serialize and deserialize the group to/from a string") {
            // get the group view
            flock_group_view_t view = FLOCK_GROUP_VIEW_INITIALIZER;
            ret = flock_group_get_view(rh1, &view);
            REQUIRE(ret == FLOCK_SUCCESS);

            // serialize it
            std::string serialized;
            ret = flock_group_view_serialize(&view,
                    [](void* u, const char* data, size_t size) {
                    auto s = static_cast<std::string*>(u);
                    *s = std::string{data, size};
                    }, &serialized);
            REQUIRE(ret == FLOCK_SUCCESS);

            // destroy original group handle
            ret = flock_group_handle_release(rh1);
            REQUIRE(ret == FLOCK_SUCCESS);

            // destroy the view
            flock_group_view_clear(&view);

            // deserialize it
            flock_group_handle_t rh;
            ret = flock_group_handle_create_from_serialized(client,
                    serialized.c_str(), serialized.size(), 0, &rh);
            REQUIRE(ret == FLOCK_SUCCESS);
            // get the view from the new handle
            ret = flock_group_get_view(rh, &view);
            REQUIRE(ret == FLOCK_SUCCESS);

            // test flock_group_view_member_count
            size_t group_size = flock_group_view_member_count(&view);
            REQUIRE(group_size == 5);

            // test iterate over members
            for(size_t i = 0; i < group_size; ++i) {
                auto member = flock_group_view_member_at(&view, i);
                REQUIRE(member->provider_id == i+1);
                REQUIRE(strcmp(member->address, self_addr) == 0);
            }

            // test flock_group_view_metadata_count
            size_t metadata_count = flock_group_view_metadata_count(&view);
            REQUIRE(metadata_count == 4);

            // test iterate over metadata
            for(size_t i = 0; i < metadata_count; ++i) {
                auto metadata = flock_group_view_metadata_at(&view, i);
                REQUIRE(metadata->key != nullptr);
                REQUIRE(metadata->value != nullptr);
            }

            REQUIRE(strcmp(flock_group_view_find_metadata(&view, "matthieu"), "dorier") == 0);
            REQUIRE(strcmp(flock_group_view_find_metadata(&view, "shane"), "snyder") == 0);

            // clear view
            flock_group_view_clear(&view);

            // destroy the group handle
            ret = flock_group_handle_release(rh);
            REQUIRE(ret == FLOCK_SUCCESS);
        }

        SECTION("Serialize and deserialize the group to/from a file") {

            // serialize it
            std::string filename = "tmp-group." + std::to_string(time(nullptr));

            // destroy the file
            remove(filename.c_str());

            // get the group view
            flock_group_view_t view = FLOCK_GROUP_VIEW_INITIALIZER;
            ret = flock_group_get_view(rh1, &view);
            REQUIRE(ret == FLOCK_SUCCESS);

            // serialize it
            std::string serialized;
            ret = flock_group_view_serialize_to_file(&view, filename.c_str());
            REQUIRE(ret == FLOCK_SUCCESS);

            // destroy original group handle
            ret = flock_group_handle_release(rh1);
            REQUIRE(ret == FLOCK_SUCCESS);

            // destroy the view
            flock_group_view_clear(&view);

            // deserialize it
            flock_group_handle_t rh;
            ret = flock_group_handle_create_from_file(client, filename.c_str(), 0, &rh);
            REQUIRE(ret == FLOCK_SUCCESS);

            // get the view from the new handle
            ret = flock_group_get_view(rh, &view);
            REQUIRE(ret == FLOCK_SUCCESS);

            // test flock_group_view_member_count
            size_t group_size = flock_group_view_member_count(&view);
            REQUIRE(group_size == 5);

            // test iterate over members
            for(size_t i = 0; i < group_size; ++i) {
                auto member = flock_group_view_member_at(&view, i);
                REQUIRE(member->provider_id == i+1);
                REQUIRE(strcmp(member->address, self_addr) == 0);
            }

            // test flock_group_view_metadata_count
            size_t metadata_count = flock_group_view_metadata_count(&view);
            REQUIRE(metadata_count == 4);

            // test iterate over metadata
            for(size_t i = 0; i < metadata_count; ++i) {
                auto metadata = flock_group_view_metadata_at(&view, i);
                REQUIRE(metadata->key != nullptr);
                REQUIRE(metadata->value != nullptr);
            }

            REQUIRE(strcmp(flock_group_view_find_metadata(&view, "matthieu"), "dorier") == 0);
            REQUIRE(strcmp(flock_group_view_find_metadata(&view, "shane"), "snyder") == 0);

            // clear view
            flock_group_view_clear(&view);

            // destroy the group handle
            ret = flock_group_handle_release(rh);
            REQUIRE(ret == FLOCK_SUCCESS);
        }
    }

    SECTION("Deserialize the group from the file generated by the providers") {

        // deserialize it
        flock_group_handle_t rh;
        ret = flock_group_handle_create_from_file(client, filename.c_str(), 0, &rh);
        REQUIRE(ret == FLOCK_SUCCESS);

        // get the view from the new handle
        flock_group_view_t view = FLOCK_GROUP_VIEW_INITIALIZER;
        ret = flock_group_get_view(rh, &view);
        REQUIRE(ret == FLOCK_SUCCESS);

        // test flock_group_view_member_count
        size_t group_size = flock_group_view_member_count(&view);
        REQUIRE(group_size == 5);

        // test iterate over members
        for(size_t i = 0; i < group_size; ++i) {
            auto member = flock_group_view_member_at(&view, i);
            REQUIRE(member->provider_id == i+1);
            REQUIRE(strcmp(member->address, self_addr) == 0);
        }

        // test flock_group_view_metadata_count
        size_t metadata_count = flock_group_view_metadata_count(&view);
        REQUIRE(metadata_count == 4);

        // test iterate over metadata
        for(size_t i = 0; i < metadata_count; ++i) {
            auto metadata = flock_group_view_metadata_at(&view, i);
            REQUIRE(metadata->key != nullptr);
            REQUIRE(metadata->value != nullptr);
        }

        REQUIRE(strcmp(flock_group_view_find_metadata(&view, "matthieu"), "dorier") == 0);
        REQUIRE(strcmp(flock_group_view_find_metadata(&view, "shane"), "snyder") == 0);

        // clear view
        flock_group_view_clear(&view);

        // destroy the group handle
        ret = flock_group_handle_release(rh);
        REQUIRE(ret == FLOCK_SUCCESS);
    }

    // destroy the group's file
    remove(filename.c_str());

    // test that we can free the client object
    ret = flock_client_finalize(client);
    REQUIRE(ret == FLOCK_SUCCESS);
}
