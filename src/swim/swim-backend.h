/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FLOCK_SWIM_BACKEND_H
#define __FLOCK_SWIM_BACKEND_H

#include "flock/flock-common.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Registers the SWIM backend.
 *
 * @return FLOCK_SUCCESS or error code.
 */
flock_return_t flock_register_swim_backend(void);

#ifdef __cplusplus
}
#endif

#endif /* __FLOCK_SWIM_BACKEND_H */
