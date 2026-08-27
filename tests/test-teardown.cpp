/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 *
 * Regression test for the simultaneous-teardown use-after-free in the dynamic
 * backends (mochi-flock issue: "provider teardown crash").
 *
 * Two group members that tear down at the same time each notify the other (a
 * SWIM LEAVE announce, or a centralized leave / membership-update RPC). Before
 * the fix, that message arrived in an RPC handler on a peer that was already
 * inside flock_provider_destroy(): the peer had freed its update-callbacks
 * lock/list (provider finalize ordering) and/or freed the backend context
 * (destroy_group not draining in-flight handlers), so the handler dereferenced
 * freed memory and jumped to a wild address on a Margo ULT.
 *
 * The test reproduces that pattern in-process for both dynamic backends: both
 * providers live on one Margo instance (distinct provider ids, same address, as
 * elsewhere in test-swim.cpp), a membership callback is registered so the
 * dispatch path walks a non-empty list, the backend protocol is allowed to run
 * for a few periods, and then both providers are destroyed concurrently from two
 * Argobots ULTs. Many iterations are run so that, under AddressSanitizer, any
 * reintroduced use-after-free is caught deterministically rather than as an
 * occasional SIGSEGV.
 */
#include <stdio.h>
#include <margo.h>
#include <abt.h>
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <flock/flock-server.h>
#include <flock/flock-group.h>
#include <flock/flock-group-view.h>
#include <atomic>

namespace {

const char* const SWIM_CONFIG = R"({
    "group":{
        "type":"swim",
        "config":{
            "protocol_period_ms": 50.0,
            "ping_timeout_ms": 30.0,
            "ping_req_timeout_ms": 60.0,
            "ping_req_members": 1,
            "suspicion_timeout_ms": 1000.0
        }
    },
    "bootstrap": "view"
})";

const char* const CENTRALIZED_CONFIG = R"({
    "group":{
        "type":"centralized",
        "config":{
            "ping_interval_ms_min": 20.0,
            "ping_interval_ms_max": 40.0,
            "ping_timeout_ms": 30.0,
            "ping_max_num_timeouts": 3
        }
    },
    "bootstrap": "view"
})";

std::atomic<uint64_t> g_membership_events{0};

void membership_cb(void* /*uargs*/, flock_update_t /*u*/,
                   const char* /*addr*/, uint16_t /*pid*/) {
    g_membership_events.fetch_add(1);
}

/* Several members on one Margo instance, all torn down concurrently. Beyond
 * exercising each backend's teardown drain, this stresses margo's
 * finalize-callback list under concurrent flock_provider_destroy on the same
 * instance, which requires the fix released in margo 0.24.2 (see the bumped
 * requirement in spack.yaml). */
const uint16_t GROUP_SIZE = 5;

/* Register a GROUP_SIZE-member group (provider ids 1..N) on a single Margo
 * instance. Every member lists all the others in its initial view so that
 * teardown produces leave notifications between them and, for the centralized
 * backend, the primary is actively pinging several secondaries. */
void register_group(margo_instance_id mid, const char* self_addr,
                    const char* config, flock_provider_t* providers) {
    for(uint16_t i = 0; i < GROUP_SIZE; ++i) {
        flock_group_view_t view = FLOCK_GROUP_VIEW_INITIALIZER;
        for(uint16_t j = 0; j < GROUP_SIZE; ++j)
            flock_group_view_add_member(&view, self_addr, (uint16_t)(j + 1));

        struct flock_provider_args args = FLOCK_PROVIDER_ARGS_INIT;
        args.initial_view = &view;

        flock_return_t ret = flock_provider_register(
            mid, (uint16_t)(i + 1), config, &args, &providers[i]);
        REQUIRE(ret == FLOCK_SUCCESS);

        ret = flock_provider_add_update_callbacks(
            providers[i], membership_cb, nullptr, providers[i]);
        REQUIRE(ret == FLOCK_SUCCESS);
    }
}

void destroy_ult(void* arg) {
    flock_provider_destroy(*static_cast<flock_provider_t*>(arg));
}

} // namespace

TEST_CASE("Providers survive simultaneous teardown", "[teardown]") {

    const char* config = GENERATE(SWIM_CONFIG, CENTRALIZED_CONFIG);

    margo_instance_id mid = margo_init("na+sm", MARGO_SERVER_MODE, 1, 2);
    REQUIRE(mid != MARGO_INSTANCE_NULL);

    hg_addr_t self = HG_ADDR_NULL;
    REQUIRE(margo_addr_self(mid, &self) == HG_SUCCESS);
    char self_addr[256];
    hg_size_t self_addr_size = sizeof(self_addr);
    REQUIRE(margo_addr_to_string(mid, self_addr, &self_addr_size, self) == HG_SUCCESS);
    margo_addr_free(mid, self);

    ABT_pool pool = ABT_POOL_NULL;
    REQUIRE(margo_get_handler_pool(mid, &pool) == HG_SUCCESS);
    REQUIRE(pool != ABT_POOL_NULL);

    // A handful of rounds is enough for ASan to catch a reintroduced UAF while
    // keeping the test fast; each round is a full setup + concurrent teardown.
    const int rounds = 20;
    for(int r = 0; r < rounds; ++r) {
        flock_provider_t providers[GROUP_SIZE];
        for(uint16_t i = 0; i < GROUP_SIZE; ++i) providers[i] = FLOCK_PROVIDER_NULL;
        register_group(mid, self_addr, config, providers);

        // Let the backend protocol exchange a few messages so timers/handlers
        // are active when teardown starts.
        margo_thread_sleep(mid, 150);

        // Destroy ALL providers at the same time: N-1 on fresh ULTs, one inline.
        // Each member's leave notification / ping lands on peers that are
        // themselves inside flock_provider_destroy().
        ABT_thread ths[GROUP_SIZE - 1];
        for(uint16_t i = 1; i < GROUP_SIZE; ++i)
            REQUIRE(ABT_thread_create(pool, destroy_ult, &providers[i],
                                      ABT_THREAD_ATTR_NULL, &ths[i - 1]) == ABT_SUCCESS);
        flock_provider_destroy(providers[0]);
        for(uint16_t i = 0; i < GROUP_SIZE - 1; ++i) {
            REQUIRE(ABT_thread_join(ths[i]) == ABT_SUCCESS);
            ABT_thread_free(&ths[i]);
        }
    }

    // If we reach here the teardown path did not crash. The callback counter is
    // not asserted on (protocol timing is nondeterministic); reaching this point
    // cleanly is the property under test.
    SUCCEED("completed " << rounds << " simultaneous teardowns without crashing");

    margo_finalize(mid);
}

TEST_CASE("Providers survive teardown from a finalization callback", "[teardown]") {

    const char* config = GENERATE(SWIM_CONFIG, CENTRALIZED_CONFIG);

    margo_instance_id mid = margo_init("na+sm", MARGO_SERVER_MODE, 1, 2);
    REQUIRE(mid != MARGO_INSTANCE_NULL);

    hg_addr_t self = HG_ADDR_NULL;
    REQUIRE(margo_addr_self(mid, &self) == HG_SUCCESS);
    char self_addr[256];
    hg_size_t self_addr_size = sizeof(self_addr);
    REQUIRE(margo_addr_to_string(mid, self_addr, &self_addr_size, self) == HG_SUCCESS);
    margo_addr_free(mid, self);

    // Register a live group and then tear it down by finalizing the instance
    // WITHOUT calling flock_provider_destroy first. flock_finalize_provider then
    // runs from a Margo finalization callback, at which point the progress loop
    // is already stopped -- so the backend teardown drain must not rely on
    // margo_thread_sleep (whose timer would never fire), or this hangs.
    flock_provider_t providers[GROUP_SIZE];
    for(uint16_t i = 0; i < GROUP_SIZE; ++i) providers[i] = FLOCK_PROVIDER_NULL;
    register_group(mid, self_addr, config, providers);

    // Let the backend protocol run so timers/handlers are active at finalize.
    margo_thread_sleep(mid, 150);

    // Finalize with the providers still registered. If the drain hangs, this
    // test times out instead of returning.
    margo_finalize(mid);

    SUCCEED("finalized with live providers without hanging");
}
