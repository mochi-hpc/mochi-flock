/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FLOCK_SWIM_GOSSIP_BUFFER_H
#define __FLOCK_SWIM_GOSSIP_BUFFER_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <mercury.h>
#include <mercury_macros.h>
#include <mercury_proc.h>
#include <mercury_proc_string.h>
#include <abt.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief SWIM gossip event types.
 */
typedef enum swim_gossip_type {
    SWIM_GOSSIP_ALIVE = 0,
    SWIM_GOSSIP_SUSPECT,
    SWIM_GOSSIP_CONFIRM,
    SWIM_GOSSIP_JOIN,
    SWIM_GOSSIP_LEAVE
} swim_gossip_type_t;

/**
 * @brief A single gossip entry describing a membership event.
 */
typedef struct swim_gossip_entry {
    swim_gossip_type_t type;
    char*              address;
    uint16_t           provider_id;
    uint64_t           incarnation;
} swim_gossip_entry_t;

/**
 * @brief Mercury serialization for swim_gossip_entry_t.
 */
static inline hg_return_t hg_proc_swim_gossip_entry_t(hg_proc_t proc, swim_gossip_entry_t* entry) {
    hg_return_t hret;
    uint8_t type_val = (uint8_t)entry->type;
    hret = hg_proc_uint8_t(proc, &type_val);
    if(hret != HG_SUCCESS) return hret;
    if(hg_proc_get_op(proc) == HG_DECODE)
        entry->type = (swim_gossip_type_t)type_val;
    hret = hg_proc_hg_string_t(proc, &entry->address);
    if(hret != HG_SUCCESS) return hret;
    hret = hg_proc_uint16_t(proc, &entry->provider_id);
    if(hret != HG_SUCCESS) return hret;
    hret = hg_proc_uint64_t(proc, &entry->incarnation);
    return hret;
}

/**
 * @brief Internal structure for gossip buffer entries.
 */
typedef struct swim_gossip_buffer_entry {
    swim_gossip_entry_t             entry;
    unsigned                        gossip_count;   // How many times this has been gossiped
    unsigned                        max_gossip;     // Max times to gossip (log(n))
    struct swim_gossip_buffer_entry* next;
} swim_gossip_buffer_entry_t;

/**
 * @brief Gossip buffer managing membership events.
 */
typedef struct swim_gossip_buffer {
    swim_gossip_buffer_entry_t* entries;
    size_t                      num_entries;
    size_t                      group_size;     // Used to compute max_gossip
    ABT_mutex_memory            mtx;
} swim_gossip_buffer_t;

/**
 * @brief Create a new gossip buffer.
 *
 * @param[out] buffer Pointer to store the created buffer.
 * @return FLOCK_SUCCESS or error code.
 */
int swim_gossip_buffer_create(swim_gossip_buffer_t** buffer);

/**
 * @brief Destroy a gossip buffer.
 *
 * @param buffer Buffer to destroy.
 */
void swim_gossip_buffer_destroy(swim_gossip_buffer_t* buffer);

/**
 * @brief Add a gossip entry to the buffer.
 *
 * @param buffer The gossip buffer.
 * @param type Event type.
 * @param address Member address.
 * @param provider_id Member provider ID.
 * @param incarnation Incarnation number.
 */
void swim_gossip_buffer_add(
    swim_gossip_buffer_t* buffer,
    swim_gossip_type_t type,
    const char* address,
    uint16_t provider_id,
    uint64_t incarnation);

/**
 * @brief Gather gossip entries to piggyback on a message.
 *
 * @param buffer The gossip buffer.
 * @param max_entries Maximum number of entries to gather.
 * @param[out] entries Array to store gathered entries (caller allocates).
 * @param[out] num_entries Number of entries gathered.
 */
void swim_gossip_buffer_gather(
    swim_gossip_buffer_t* buffer,
    size_t max_entries,
    swim_gossip_entry_t* entries,
    size_t* num_entries);

/**
 * @brief Update the group size (affects how long entries are gossiped).
 *
 * @param buffer The gossip buffer.
 * @param group_size New group size.
 */
void swim_gossip_buffer_set_group_size(
    swim_gossip_buffer_t* buffer,
    size_t group_size);

/**
 * @brief Remove old entries that have been gossiped enough times.
 *
 * @param buffer The gossip buffer.
 */
void swim_gossip_buffer_cleanup(swim_gossip_buffer_t* buffer);

/**
 * @brief Compute log2(n) * 3 ceiling for max gossip count.
 *
 * @param group_size Group size.
 * @return Max gossip count.
 */
unsigned swim_compute_max_gossip(size_t group_size);

#ifdef __cplusplus
}
#endif

#endif /* __FLOCK_SWIM_GOSSIP_BUFFER_H */
