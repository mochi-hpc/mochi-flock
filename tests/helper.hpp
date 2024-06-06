/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdio.h>
#include <margo.h>
#include <flock/flock-server.h>
#include <flock/flock-client.h>
#include <flock/flock-group.h>
#include <stdexcept>
#include <vector>
#include <iostream>

struct TestGroup {

    TestGroup(margo_instance_id mid,
              size_t group_size,
              const char* provider_config = R"({
                "group":{
                    "type":"static",
                    "config":{}
                }
              })") {

        // get address of current process
        hg_addr_t addr = HG_ADDR_NULL;
        hg_return_t hret = margo_addr_self(mid, &addr);
        if(hret != HG_SUCCESS)
            throw std::runtime_error("margo_addr_self failed when creating TestGroup");

        // get self address as a string
        char address[256];
        hg_size_t address_size = 256;
        hret = margo_addr_to_string(mid, address, &address_size, addr);
        if(hret != HG_SUCCESS)
            throw std::runtime_error("margo_addr_to_string failed when creating TestGroup");
        margo_addr_free(mid, addr);

        // register flock providers
        providers.resize(group_size, nullptr);
        for(size_t i = 0; i < group_size; ++i) {
            // IMPORTANT: flock_provider_bootstrap will take ownerwhip of the view
            // we are creating, so we have to re-create it for every provider

            // create the initial group view to bootstrap with
            flock_group_view_t initial_view = FLOCK_GROUP_VIEW_INITIALIZER;
            for(size_t j = 0; j < group_size; ++j)
                flock_group_view_add_member(&initial_view, address, (uint16_t)(j+1));

            // add some metadata
            flock_group_view_add_metadata(&initial_view, "matthieu", "dorier");
            flock_group_view_add_metadata(&initial_view, "shane", "snyder");

            struct flock_provider_args args = FLOCK_PROVIDER_ARGS_INIT;
            args.initial_view = &initial_view;

            flock_return_t ret = flock_provider_register(
                    mid, i+1, provider_config, &args,
                    &providers[i]);
            if(ret != FLOCK_SUCCESS)
                throw std::runtime_error(
                    "flock_provider_register failed when initializing TestGroup: code="
                    + std::to_string(ret));
        }

    }

    virtual ~TestGroup() {
        for(auto provider : providers)
            flock_provider_destroy(provider);
    }

    std::vector<flock_provider_t> providers;
};
