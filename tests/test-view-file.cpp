/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 *
 * Regression test for concurrent (de)serialization of a group view to/from a
 * file (mochi-flock issue: "group file corruption / garbage address under
 * concurrent joins").
 *
 * The provider re-serializes the group view to a file on every membership
 * change. Two defects made this unsafe when more than one thread drove those
 * changes:
 *   (1) the serializer iterated the members array WITHOUT holding the view lock,
 *       so a concurrent add/remove (which reallocs/frees the array) could be
 *       read mid-update, producing a garbage member address in the file; and
 *   (2) the temporary file used a fixed "<name>.swp" name, so concurrent
 *       serializations collided on one inode and could publish a truncated file.
 * A reader then hit "[flock] JSON parse error" or a "Malformed FI_ADDR_STR".
 *
 * This test drives several serializer ULTs, a view-modifier ULT and several
 * reader ULTs in parallel on a multi-execution-stream pool. Under AddressSanitizer
 * defect (1) shows up as a heap-use-after-free in the serializer; without a
 * sanitizer both defects show up as reader parse failures or malformed addresses.
 * With the fixes the readers always parse a complete, self-consistent file.
 */
#include <stdio.h>
#include <string.h>
#include <margo.h>
#include <abt.h>
#include <catch2/catch_test_macros.hpp>
#include <flock/flock-group-view.h>
#include <atomic>
#include <vector>

namespace {

const char* const TMP_PATH = "test-view-file.tmp.json";

std::atomic<bool>     g_stop{false};
std::atomic<uint64_t> g_reads_ok{0};   // successful, well-formed parses
std::atomic<uint64_t> g_parse_fail{0}; // a well-formed file should never fail to parse
std::atomic<uint64_t> g_bad_addr{0};   // garbage/unexpected address in a parsed view

struct shared {
    flock_group_view_t* view;
    const char*         path;
    const char*         self_addr;
};

void writer_ult(void* arg) {
    shared* s = (shared*)arg;
    while(!g_stop.load(std::memory_order_relaxed))
        flock_group_view_serialize_to_file(s->view, s->path);
}

/* Add then remove a transient member, exactly as the backends do: under the
 * view lock. This is what races the (previously unlocked) serializer. */
void modifier_ult(void* arg) {
    shared* s = (shared*)arg;
    uint16_t pid = 100;
    while(!g_stop.load(std::memory_order_relaxed)) {
        FLOCK_GROUP_VIEW_LOCK(s->view);
        flock_group_view_add_member(s->view, s->self_addr, pid);
        FLOCK_GROUP_VIEW_UNLOCK(s->view);

        FLOCK_GROUP_VIEW_LOCK(s->view);
        flock_member_t* m = flock_group_view_find_member(s->view, s->self_addr, pid);
        if(m) flock_group_view_remove_member(s->view, m);
        FLOCK_GROUP_VIEW_UNLOCK(s->view);

        if(++pid > 200) pid = 100;
    }
}

void reader_ult(void* arg) {
    shared* s = (shared*)arg;
    while(!g_stop.load(std::memory_order_relaxed)) {
        flock_group_view_t v = FLOCK_GROUP_VIEW_INITIALIZER;
        flock_return_t ret = flock_group_view_from_file(s->path, &v);
        if(ret != FLOCK_SUCCESS) {
            // The file is always published atomically (rename), so a well-formed
            // reader must never see a partial/corrupt file.
            g_parse_fail.fetch_add(1);
        } else {
            size_t n = flock_group_view_member_count(&v);
            for(size_t i = 0; i < n; ++i) {
                flock_member_t* m = flock_group_view_member_at(&v, i);
                if(!m || !m->address || m->address[0] == '\0'
                        || strcmp(m->address, s->self_addr) != 0) {
                    g_bad_addr.fetch_add(1);
                }
            }
            g_reads_ok.fetch_add(1);
        }
        flock_group_view_clear(&v);
    }
}

} // namespace

TEST_CASE("Concurrent group-view file serialize/deserialize", "[view-file]") {

    // Use several RPC handler ESs so the ULTs below run truly in parallel.
    margo_instance_id mid = margo_init("na+sm", MARGO_SERVER_MODE, 1, 4);
    REQUIRE(mid != MARGO_INSTANCE_NULL);

    ABT_pool pool = ABT_POOL_NULL;
    REQUIRE(margo_get_handler_pool(mid, &pool) == HG_SUCCESS);
    REQUIRE(pool != ABT_POOL_NULL);

    hg_addr_t self = HG_ADDR_NULL;
    REQUIRE(margo_addr_self(mid, &self) == HG_SUCCESS);
    char self_addr[256];
    hg_size_t self_addr_size = sizeof(self_addr);
    REQUIRE(margo_addr_to_string(mid, self_addr, &self_addr_size, self) == HG_SUCCESS);
    margo_addr_free(mid, self);

    // A base view with a few stable members (all sharing this process' address).
    flock_group_view_t view = FLOCK_GROUP_VIEW_INITIALIZER;
    for(uint16_t i = 1; i <= 5; ++i)
        flock_group_view_add_member(&view, self_addr, i);
    flock_group_view_add_metadata(&view, "k1", "v1");
    flock_group_view_add_metadata(&view, "k2", "v2");

    // Publish once so the file exists before the readers start.
    REQUIRE(flock_group_view_serialize_to_file(&view, TMP_PATH) == FLOCK_SUCCESS);

    shared s{&view, TMP_PATH, self_addr};
    g_stop.store(false);
    g_reads_ok.store(0);
    g_parse_fail.store(0);
    g_bad_addr.store(0);

    const int NW = 3, NR = 3;
    std::vector<ABT_thread> ths;
    for(int i = 0; i < NW; ++i) {
        ABT_thread t;
        REQUIRE(ABT_thread_create(pool, writer_ult, &s, ABT_THREAD_ATTR_NULL, &t) == ABT_SUCCESS);
        ths.push_back(t);
    }
    {
        ABT_thread t;
        REQUIRE(ABT_thread_create(pool, modifier_ult, &s, ABT_THREAD_ATTR_NULL, &t) == ABT_SUCCESS);
        ths.push_back(t);
    }
    for(int i = 0; i < NR; ++i) {
        ABT_thread t;
        REQUIRE(ABT_thread_create(pool, reader_ult, &s, ABT_THREAD_ATTR_NULL, &t) == ABT_SUCCESS);
        ths.push_back(t);
    }

    // Let the storm run, then stop and join.
    margo_thread_sleep(mid, 1500);
    g_stop.store(true);
    for(auto& t : ths) {
        REQUIRE(ABT_thread_join(t) == ABT_SUCCESS);
        ABT_thread_free(&t);
    }

    INFO("successful reads=" << g_reads_ok.load()
         << " parse failures=" << g_parse_fail.load()
         << " bad addresses=" << g_bad_addr.load());
    CHECK(g_reads_ok.load() > 0);     // readers made progress
    CHECK(g_parse_fail.load() == 0);  // every published file parsed cleanly
    CHECK(g_bad_addr.load() == 0);    // no garbage/torn address ever surfaced

    flock_group_view_clear(&view);
    remove(TMP_PATH);
    margo_finalize(mid);
}

TEST_CASE("from_file rejects empty, truncated and missing files", "[view-file]") {

    // Empty file: must be a clean error, not a crash and not a bogus success.
    {
        FILE* f = fopen("test-view-empty.tmp.json", "w");
        REQUIRE(f != nullptr);
        fclose(f);
        flock_group_view_t v = FLOCK_GROUP_VIEW_INITIALIZER;
        flock_return_t ret = flock_group_view_from_file("test-view-empty.tmp.json", &v);
        CHECK(ret != FLOCK_SUCCESS);
        flock_group_view_clear(&v);
        remove("test-view-empty.tmp.json");
    }

    // Truncated JSON: must be a clean parse error, not garbage.
    {
        FILE* f = fopen("test-view-bad.tmp.json", "w");
        REQUIRE(f != nullptr);
        fputs("{ \"members\": [ { \"address\": \"na+sm://1-0\"", f); // no closing braces
        fclose(f);
        flock_group_view_t v = FLOCK_GROUP_VIEW_INITIALIZER;
        flock_return_t ret = flock_group_view_from_file("test-view-bad.tmp.json", &v);
        CHECK(ret != FLOCK_SUCCESS);
        flock_group_view_clear(&v);
        remove("test-view-bad.tmp.json");
    }

    // Missing file.
    {
        flock_group_view_t v = FLOCK_GROUP_VIEW_INITIALIZER;
        flock_return_t ret = flock_group_view_from_file("test-view-missing.tmp.json", &v);
        CHECK(ret != FLOCK_SUCCESS);
        flock_group_view_clear(&v);
    }
}
