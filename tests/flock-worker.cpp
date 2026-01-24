/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

/*
 * Worker process for the interactive Flock backend tester.
 *
 * This program runs a single Flock provider that joins a group.
 * Communication with the parent process is via:
 * - stdout: Prints the Margo address on startup
 * - control FIFO: Receives "STOP" command for graceful shutdown
 *
 * Usage:
 *     flock-worker <transport> <provider_config> <group_file> <control_fifo>
 *
 * Arguments:
 *     transport: Margo transport (e.g., "na+sm")
 *     provider_config: JSON string with group configuration
 *     group_file: JSON file for group view (created if first worker, read otherwise)
 *     control_fifo: Named pipe path for receiving stop commands
 */

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <cstdlib>
#include <cstring>
#include <csignal>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <margo.h>
#include <flock/flock-server.h>
#include <flock/flock-group-view.h>
#include <flock/flock-bootstrap.h>

int main(int argc, char** argv)
{
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0]
                  << " <transport> <provider_config> <group_file> <control_fifo>"
                  << std::endl;
        return 1;
    }

    const char* transport = argv[1];
    const char* provider_config = argv[2];
    const char* group_file = argv[3];
    const char* control_fifo = argv[4];

    // Create Margo engine
    margo_instance_id mid = margo_init(transport, MARGO_SERVER_MODE, 1, -1);
    if (mid == MARGO_INSTANCE_NULL) {
        std::cerr << "Error: Failed to initialize Margo" << std::endl;
        return 1;
    }

    // Get self address
    hg_addr_t self_addr;
    hg_return_t hret = margo_addr_self(mid, &self_addr);
    if (hret != HG_SUCCESS) {
        std::cerr << "Error: Failed to get self address" << std::endl;
        margo_finalize(mid);
        return 1;
    }

    char address[256];
    hg_size_t address_size = sizeof(address);
    hret = margo_addr_to_string(mid, address, &address_size, self_addr);
    margo_addr_free(mid, self_addr);

    if (hret != HG_SUCCESS) {
        std::cerr << "Error: Failed to convert address to string" << std::endl;
        margo_finalize(mid);
        return 1;
    }

    // Print address to stdout (parent reads this)
    std::cout << address << std::endl;
    std::cout.flush();

    // Load or create initial view
    flock_group_view_t initial_view = FLOCK_GROUP_VIEW_INITIALIZER;
    flock_return_t ret;

    // Check if group file exists
    struct stat st;
    if (stat(group_file, &st) == 0) {
        // Join existing group - load view from file
        ret = flock_group_view_from_file(group_file, &initial_view);
        if (ret != FLOCK_SUCCESS) {
            std::cerr << "Error: Failed to load group view from file" << std::endl;
            margo_finalize(mid);
            return 1;
        }
    } else {
        // First worker - create new group with just ourselves
        if (!flock_group_view_add_member(&initial_view, address, 0)) {
            std::cerr << "Error: Failed to create initial group view" << std::endl;
            margo_finalize(mid);
            return 1;
        }
    }

    // The initial_view will be moved into the provider and be empty
    // after the call so we need to copy it beforehand
    flock_group_view_t view_copy = FLOCK_GROUP_VIEW_INITIALIZER;
    flock_group_view_copy(&initial_view, &view_copy);

    // Create provider (joins the group)
    flock_provider_args args = FLOCK_PROVIDER_ARGS_INIT;
    args.initial_view = &initial_view;

    flock_provider_t provider = FLOCK_PROVIDER_NULL;
    ret = flock_provider_register(mid, 0, provider_config, &args, &provider);
    if (ret != FLOCK_SUCCESS) {
        std::cerr << "Error: Failed to register provider (error code: " << ret << ")" << std::endl;
        flock_group_view_clear(&initial_view);
        margo_finalize(mid);
        return 1;
    }

    // Move the copy of the view back into initial_view
    FLOCK_GROUP_VIEW_MOVE(&view_copy, &initial_view);

    // Save updated group view to file for other workers
    ret = flock_group_view_serialize_to_file(&initial_view, group_file);
    if (ret != FLOCK_SUCCESS) {
        std::cerr << "Warning: Failed to save group view to file" << std::endl;
    }

    // Block reading from control FIFO
    // On "STOP": delete provider gracefully, exit
    // On SIGKILL: dies immediately (crash simulation)
    int fifo_fd = open(control_fifo, O_RDONLY);
    if (fifo_fd < 0) {
        std::cerr << "Error: Failed to open control FIFO" << std::endl;
        flock_provider_destroy(provider);
        margo_finalize(mid);
        return 1;
    }

    char buffer[256];
    while (true) {
        ssize_t n = read(fifo_fd, buffer, sizeof(buffer) - 1);
        if (n <= 0) {
            // EOF or error - parent closed FIFO or error occurred
            break;
        }
        buffer[n] = '\0';

        // Trim newline
        char* newline = strchr(buffer, '\n');
        if (newline) *newline = '\0';

        if (strcmp(buffer, "STOP") == 0) {
            break;
        }
    }

    close(fifo_fd);

    // Graceful shutdown
    flock_provider_destroy(provider);
    margo_finalize(mid);

    return 0;
}
