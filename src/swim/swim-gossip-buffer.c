/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "swim-gossip-buffer.h"
#include "flock/flock-common.h"

/* Compute log2(n) ceiling for max gossip count */
unsigned swim_compute_max_gossip(size_t group_size) {
    if(group_size <= 1) return 1;
    unsigned log2_n = 0;
    size_t n = group_size - 1;
    while(n > 0) {
        log2_n++;
        n >>= 1;
    }
    /* Return at least 3 * log2(n) for good dissemination */
    return log2_n * 3;
}

int swim_gossip_buffer_create(swim_gossip_buffer_t** buffer) {
    swim_gossip_buffer_t* buf = (swim_gossip_buffer_t*)calloc(1, sizeof(*buf));
    if(!buf) return FLOCK_ERR_ALLOCATION;
    buf->entries = NULL;
    buf->num_entries = 0;
    buf->group_size = 1;
    *buffer = buf;
    return FLOCK_SUCCESS;
}

void swim_gossip_buffer_destroy(swim_gossip_buffer_t* buffer) {
    if(!buffer) return;
    ABT_mutex_lock(ABT_MUTEX_MEMORY_GET_HANDLE(&buffer->mtx));
    swim_gossip_buffer_entry_t* entry = buffer->entries;
    while(entry) {
        swim_gossip_buffer_entry_t* next = entry->next;
        free(entry->entry.address);
        free(entry);
        entry = next;
    }
    ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&buffer->mtx));
    free(buffer);
}

void swim_gossip_buffer_add(
    swim_gossip_buffer_t* buffer,
    swim_gossip_type_t type,
    const char* address,
    uint16_t provider_id,
    uint64_t incarnation)
{
    ABT_mutex_lock(ABT_MUTEX_MEMORY_GET_HANDLE(&buffer->mtx));

    /* Check if we already have an entry for this member */
    swim_gossip_buffer_entry_t* existing = buffer->entries;
    while(existing) {
        if(existing->entry.provider_id == provider_id &&
           strcmp(existing->entry.address, address) == 0) {
            /* Only update if the new incarnation is higher or type has higher priority */
            if(incarnation > existing->entry.incarnation ||
               (incarnation == existing->entry.incarnation && type > existing->entry.type)) {
                existing->entry.type = type;
                existing->entry.incarnation = incarnation;
                existing->gossip_count = 0; /* Reset gossip count for new info */
            }
            ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&buffer->mtx));
            return;
        }
        existing = existing->next;
    }

    /* Create a new entry */
    swim_gossip_buffer_entry_t* new_entry =
        (swim_gossip_buffer_entry_t*)calloc(1, sizeof(*new_entry));
    if(!new_entry) {
        ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&buffer->mtx));
        return;
    }
    new_entry->entry.type = type;
    new_entry->entry.address = strdup(address);
    new_entry->entry.provider_id = provider_id;
    new_entry->entry.incarnation = incarnation;
    new_entry->gossip_count = 0;
    new_entry->max_gossip = swim_compute_max_gossip(buffer->group_size);

    /* Add to front of list */
    new_entry->next = buffer->entries;
    buffer->entries = new_entry;
    buffer->num_entries++;

    ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&buffer->mtx));
}

void swim_gossip_buffer_gather(
    swim_gossip_buffer_t* buffer,
    size_t max_entries,
    swim_gossip_entry_t* entries,
    size_t* num_entries)
{
    ABT_mutex_lock(ABT_MUTEX_MEMORY_GET_HANDLE(&buffer->mtx));

    size_t count = 0;
    swim_gossip_buffer_entry_t* entry = buffer->entries;

    /* Gather entries that haven't been gossiped too many times */
    while(entry && count < max_entries) {
        if(entry->gossip_count < entry->max_gossip) {
            entries[count].type = entry->entry.type;
            entries[count].address = entry->entry.address; /* Shallow copy */
            entries[count].provider_id = entry->entry.provider_id;
            entries[count].incarnation = entry->entry.incarnation;
            entry->gossip_count++;
            count++;
        }
        entry = entry->next;
    }

    *num_entries = count;

    ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&buffer->mtx));
}

void swim_gossip_buffer_set_group_size(
    swim_gossip_buffer_t* buffer,
    size_t group_size)
{
    ABT_mutex_lock(ABT_MUTEX_MEMORY_GET_HANDLE(&buffer->mtx));
    buffer->group_size = group_size;
    /* Update max_gossip for all entries */
    unsigned new_max = swim_compute_max_gossip(group_size);
    swim_gossip_buffer_entry_t* entry = buffer->entries;
    while(entry) {
        entry->max_gossip = new_max;
        entry = entry->next;
    }
    ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&buffer->mtx));
}

void swim_gossip_buffer_cleanup(swim_gossip_buffer_t* buffer) {
    ABT_mutex_lock(ABT_MUTEX_MEMORY_GET_HANDLE(&buffer->mtx));

    swim_gossip_buffer_entry_t** pp = &buffer->entries;
    while(*pp) {
        swim_gossip_buffer_entry_t* entry = *pp;
        if(entry->gossip_count >= entry->max_gossip) {
            *pp = entry->next;
            free(entry->entry.address);
            free(entry);
            buffer->num_entries--;
        } else {
            pp = &entry->next;
        }
    }

    ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&buffer->mtx));
}
