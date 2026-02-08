/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FLOCK_SWIM_BACKEND_H
#define __FLOCK_SWIM_BACKEND_H

#include <stdbool.h>
#include "flock/flock-common.h"
#include "flock/flock-server.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Registers the SWIM backend.
 *
 * @return FLOCK_SUCCESS or error code.
 */
flock_return_t flock_register_swim_backend(void);

/**
 * @brief Set crash mode for a SWIM provider.
 *
 * When crash mode is enabled, the provider will not send a LEAVE announcement
 * when destroyed, simulating a crash scenario. This is useful for testing
 * failure detection.
 *
 * @param provider The provider handle.
 * @param crash True to enable crash mode, false to disable.
 *
 * @return FLOCK_SUCCESS or error code.
 */
flock_return_t flock_swim_set_crash_mode(flock_provider_t provider, bool crash);

#ifdef __cplusplus
}
#endif

#endif /* __FLOCK_SWIM_BACKEND_H */
