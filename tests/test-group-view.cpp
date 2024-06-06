/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdio.h>
#include <margo.h>
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <flock/flock-group-view.h>
#include <unordered_map>

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

struct Member {
    std::string address;
    uint16_t    provider_id;
};

TEST_CASE("Test group view interface", "[group-view]") {

    // create test context
    auto context = std::make_unique<TestContext>();
    bool b;

    SECTION("Create group view") {

        flock_group_view_t view = FLOCK_GROUP_VIEW_INITIALIZER;
        REQUIRE(view.members.size == 0);
        REQUIRE(view.members.capacity == 0);
        REQUIRE(view.members.data == nullptr);
        REQUIRE(view.metadata.size == 0);
        REQUIRE(view.metadata.capacity == 0);
        REQUIRE(view.metadata.data == nullptr);
        REQUIRE(view.digest == 0);

        std::vector<Member> members_ref(16);
        auto previous_digest = view.digest;
        for(size_t i = 0; i < 16; ++i) {
            char address[64];
            sprintf(address, "address/%02lu", i);
            uint16_t provider_id = i+42;
            auto mem = flock_group_view_add_member(&view, address, provider_id);
            REQUIRE(mem);
            REQUIRE(view.digest != previous_digest);
            members_ref[i] = {address, provider_id};
            previous_digest = view.digest;
        }

        std::unordered_map<std::string, std::string> metadata_ref;
        for(size_t i = 0; i < 8; ++i) {
            char key[64];
            char value[64];
            sprintf(key+1, "_key%lu", i);
            key[0] = 'A' + ((i*3)%8);
            sprintf(value, "value_%lu", i);
            b = flock_group_view_add_metadata(&view, key, value);
            REQUIRE(b);
            REQUIRE(view.digest != previous_digest);
            metadata_ref.insert({key,value});
            previous_digest = view.digest;
        }

        REQUIRE(view.members.size == 16);
        REQUIRE(view.members.capacity >= 16);
        REQUIRE(view.members.data != nullptr);
        REQUIRE(view.metadata.size == 8);
        REQUIRE(view.metadata.capacity >= 8);
        REQUIRE(view.metadata.data != nullptr);

        for(size_t i = 0; i < view.members.size; ++i) {
            const flock_member_t* member = flock_group_view_member_at(&view, i);
            REQUIRE(members_ref[i].address == member->address);
            REQUIRE(members_ref[i].provider_id == member->provider_id);
        }

        const flock_member_t* member = flock_group_view_member_at(&view, view.members.size);
        REQUIRE(member == nullptr);

        for(auto& p : metadata_ref) {
            const char* value = flock_group_view_find_metadata(&view, p.first.c_str());
            REQUIRE(value != nullptr);
            REQUIRE(p.second == value);
        }

        const char* value = flock_group_view_find_metadata(&view, "abcd");
        REQUIRE(value == nullptr);

        // Try to remove a member using an invalid pointer
        previous_digest = view.digest;
        auto b = flock_group_view_remove_member(&view, (flock_member_t*)0x4);
        REQUIRE(!b);
        REQUIRE(previous_digest == view.digest);

        // Remove rank 5
        b = flock_group_view_remove_member(&view, flock_group_view_member_at(&view, 5));
        REQUIRE(b);
        REQUIRE(previous_digest != view.digest);

        REQUIRE(view.members.size == 15);

        for(size_t i = 0; i < members_ref.size(); ++i) {
            const flock_member_t* member = flock_group_view_find_member(
                    &view, members_ref[i].address.c_str(), members_ref[i].provider_id);
            if(i == 5) {
                REQUIRE(member == nullptr);
                continue;
            }
            REQUIRE(members_ref[i].address == member->address);
            REQUIRE(members_ref[i].provider_id == member->provider_id);
        }

        // Try to remove a metadata that does not exist
        previous_digest = view.digest;
        b = flock_group_view_remove_metadata(&view, "abcd");
        REQUIRE(!b);
        REQUIRE(previous_digest == view.digest);

        // Remove a metadata that does exist
        auto key_to_remove = metadata_ref.begin()->first;
        b = flock_group_view_remove_metadata(&view, key_to_remove.c_str());
        REQUIRE(b);
        REQUIRE(previous_digest != view.digest);

        for(auto& p : metadata_ref) {
            const char* value = flock_group_view_find_metadata(&view, p.first.c_str());
            if(p.first == key_to_remove) {
                REQUIRE(value == nullptr);
                continue;
            }
            REQUIRE(value != nullptr);
            REQUIRE(p.second == value);
        }

        auto members_data = view.members.data;
        auto members_size = view.members.size;
        auto members_capa = view.members.capacity;

        auto metadata_data = view.metadata.data;
        auto metadata_size = view.metadata.size;
        auto metadata_capa = view.metadata.capacity;

        auto digest = view.digest;

        // Move the view to another destination
        flock_group_view_t view2 = FLOCK_GROUP_VIEW_INITIALIZER;
        FLOCK_GROUP_VIEW_MOVE(&view, &view2);

        REQUIRE(view.members.size == 0);
        REQUIRE(view.members.capacity == 0);
        REQUIRE(view.members.data == nullptr);
        REQUIRE(view.metadata.size == 0);
        REQUIRE(view.metadata.capacity == 0);
        REQUIRE(view.metadata.data == nullptr);
        REQUIRE(view.digest == 0);

        REQUIRE(view2.members.size == members_size);
        REQUIRE(view2.members.capacity == members_capa);
        REQUIRE(view2.members.data == members_data);
        REQUIRE(view2.metadata.size == metadata_size);
        REQUIRE(view2.metadata.capacity == metadata_capa);
        REQUIRE(view2.metadata.data == metadata_data);
        REQUIRE(view2.digest == digest);

        // Clear the view2
        flock_group_view_clear(&view);
        flock_group_view_clear(&view2);

        REQUIRE(view2.members.size == 0);
        REQUIRE(view2.members.capacity == 0);
        REQUIRE(view2.members.data == nullptr);
        REQUIRE(view2.metadata.size == 0);
        REQUIRE(view2.metadata.capacity == 0);
        REQUIRE(view2.metadata.data == nullptr);
        REQUIRE(view2.digest == 0);
    }
}
