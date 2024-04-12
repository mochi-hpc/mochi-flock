/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <assert.h>
#include <stdio.h>
#include <margo.h>
#include <flock/flock-server.h>

int main(int argc, char** argv)
{
    (void)argc;
    (void)argv;
    margo_instance_id mid = margo_init("na+sm", MARGO_SERVER_MODE, 0, 0);
    assert(mid);

    margo_set_log_level(mid, MARGO_LOG_INFO);

    hg_addr_t my_address;
    margo_addr_self(mid, &my_address);
    char addr_str[128];
    size_t addr_str_size = 128;
    margo_addr_to_string(mid, addr_str, &addr_str_size, my_address);
    margo_addr_free(mid,my_address);
    margo_info(mid, "Server running at address %s, with provider id 42", addr_str);

    struct flock_provider_args args = FLOCK_PROVIDER_ARGS_INIT;
    flock_group_view_t initial_view = FLOCK_GROUP_VIEW_INITIALIZER;
    flock_group_view_add_member(&initial_view, 0, 42, addr_str);

    const char* config = "{ \"group\":{ \"type\":\"static\", \"config\":{} } }";

    flock_provider_bootstrap(mid, 42, &initial_view, config, &args, FLOCK_PROVIDER_IGNORE);

    margo_wait_for_finalize(mid);

    return 0;
}
