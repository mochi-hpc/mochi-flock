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
#include <flock/flock-bootstrap-mpi.h>

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
    uint64_t    rank;
    std::string address;
    uint16_t    provider_id;
};

TEST_CASE("Test bootstrap with MPI", "[mpi-bootstrap]") {

    MPI_Init(NULL, NULL);
    int size, rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // create test context
    auto context = std::make_unique<TestContext>();

    SECTION("Create group from MPI") {

        flock_group_view_t view = FLOCK_GROUP_VIEW_INITIALIZER;
        REQUIRE(view.members.size == 0);
        REQUIRE(view.members.capacity == 0);
        REQUIRE(view.members.data == nullptr);
        REQUIRE(view.metadata.size == 0);
        REQUIRE(view.metadata.capacity == 0);
        REQUIRE(view.metadata.data == nullptr);
        REQUIRE(view.digest == 0);

        flock_return_t ret = flock_group_view_init_from_mpi(context->mid, 42+rank, MPI_COMM_WORLD, &view);
        REQUIRE(ret == FLOCK_SUCCESS);

        REQUIRE(view.members.size == (unsigned)size);
        REQUIRE(view.members.data != nullptr);

        auto me = flock_group_view_find_member(&view, rank);
        REQUIRE(me->provider_id == 42 + rank);
        REQUIRE(me->rank == (unsigned)rank);

        char self_addr[256];
        hg_size_t self_addr_size = 256;
        margo_addr_to_string(context->mid, self_addr, &self_addr_size, context->addr);

        REQUIRE(strcmp(me->address, self_addr) == 0);

        flock_group_view_clear(&view);
    }

    MPI_Finalize();
}
