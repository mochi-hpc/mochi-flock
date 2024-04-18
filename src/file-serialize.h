/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef _FILE_SERIALIZE_H
#define _FILE_SERIALIZE_H

#include <stdlib.h>
#include <stdio.h>
#include "flock/flock-common.h"
#include "flock/flock-group-view.h"

struct file_serializer_data {
    const char*    filename;
    flock_return_t ret;
};

static inline void file_serializer(void* uargs, const char* content, size_t size)
{
    struct file_serializer_data* data = (struct file_serializer_data*)uargs;
    FILE* file = fopen(data->filename, "w");
    if(!file) {
        data->ret = FLOCK_ERR_ALLOCATION;
        return;
    }
    size_t written = fwrite(content, 1, size, file);
    if(written != size) {
        data->ret = FLOCK_ERR_OTHER;
        return;
    }
    fclose(file);
}

#endif
