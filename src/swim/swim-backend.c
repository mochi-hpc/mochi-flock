/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <string.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <json-c/json.h>
#include <margo-timer.h>
#include "flock/flock-backend.h"
#include "flock/flock-group-view.h"
#include "../provider.h"
#include "swim-backend.h"
#include "swim-gossip-buffer.h"

#define SWIM_MAX_GOSSIP_ENTRIES 8

/**
 * @brief SWIM member status.
 */
typedef enum swim_member_status {
    SWIM_ALIVE = 0,
    SWIM_SUSPECTED,
    SWIM_CONFIRMED_DEAD
} swim_member_status_t;

/* Forward declare swim_context */
struct swim_context;

/**
 * @brief Per-member state stored in flock_member_t.extra.
 */
typedef struct swim_member_state {
    struct swim_context* context;  /* Pointer to context for cleanup */
    swim_member_status_t status;
    uint64_t             incarnation;
    double               suspicion_start;
    hg_addr_t            address;
} swim_member_state_t;

/**
 * @brief Main SWIM context.
 */
typedef struct swim_context {
    /* Margo/provider info */
    margo_instance_id mid;
    uint16_t          provider_id;
    ABT_pool          pool;

    /* Self identity */
    char*    self_address;
    uint64_t self_incarnation;

    /* Group view */
    flock_group_view_t view;

    /* Configuration */
    struct json_object* config;
    ABT_mutex_memory    config_mtx;
    double   protocol_period_ms;
    double   ping_timeout_ms;
    double   ping_req_timeout_ms;
    unsigned ping_req_members;
    double   suspicion_timeout_ms;

    /* Protocol state */
    size_t*       probe_order;      /* Shuffled array of indices for round-robin probing */
    size_t        probe_order_size;
    size_t        probe_index;
    margo_timer_t protocol_timer;
    _Atomic bool  shutting_down;

    /* Gossip buffer */
    swim_gossip_buffer_t* gossip_buffer;

    /* RPC IDs */
    hg_id_t ping_rpc_id;
    hg_id_t ping_req_rpc_id;
    hg_id_t announce_rpc_id;

    /* Callbacks */
    flock_membership_update_fn member_update_callback;
    flock_metadata_update_fn   metadata_update_callback;
    void*                      callback_context;
} swim_context;

/* Forward declarations */
static flock_return_t swim_destroy_group(void* ctx);
static void protocol_timer_callback(void* args);
static void shuffle_probe_order(swim_context* ctx);
static size_t select_random_members(swim_context* ctx, size_t exclude_idx, size_t* indices, size_t count);
static void process_gossip_entries(swim_context* ctx, swim_gossip_entry_t* entries, size_t num_entries);
static void mark_member_suspected(swim_context* ctx, size_t member_idx);
static void mark_member_dead(swim_context* ctx, size_t member_idx);
static void check_suspicion_timeouts(swim_context* ctx);
static void refute_suspicion(swim_context* ctx);

/* Member state helpers */
static inline void swim_member_state_free(void* args) {
    swim_member_state_t* state = (swim_member_state_t*)args;
    if(!state) return;
    if(state->address != HG_ADDR_NULL && state->context) {
        margo_addr_free(state->context->mid, state->address);
        state->address = HG_ADDR_NULL;
    }
    free(state);
}

static inline swim_member_state_t* get_member_state(flock_member_t* member) {
    return (swim_member_state_t*)member->extra.data;
}

/* ============================================================================
 * RPC TYPES AND HANDLERS
 * ============================================================================ */

/* Ping RPC input/output */
typedef struct swim_ping_in {
    uint64_t            sender_incarnation;
    size_t              num_gossip_entries;
    swim_gossip_entry_t gossip_entries[SWIM_MAX_GOSSIP_ENTRIES];
} swim_ping_in_t;

typedef struct swim_ping_out {
    uint64_t            responder_incarnation;
    size_t              num_gossip_entries;
    swim_gossip_entry_t gossip_entries[SWIM_MAX_GOSSIP_ENTRIES];
} swim_ping_out_t;

static inline hg_return_t hg_proc_swim_ping_in_t(hg_proc_t proc, void* data) {
    swim_ping_in_t* in = (swim_ping_in_t*)data;
    hg_return_t hret;
    hret = hg_proc_uint64_t(proc, &in->sender_incarnation);
    if(hret != HG_SUCCESS) return hret;
    hret = hg_proc_hg_size_t(proc, &in->num_gossip_entries);
    if(hret != HG_SUCCESS) return hret;
    for(size_t i = 0; i < in->num_gossip_entries && i < SWIM_MAX_GOSSIP_ENTRIES; i++) {
        hret = hg_proc_swim_gossip_entry_t(proc, &in->gossip_entries[i]);
        if(hret != HG_SUCCESS) return hret;
    }
    return HG_SUCCESS;
}

static inline hg_return_t hg_proc_swim_ping_out_t(hg_proc_t proc, void* data) {
    swim_ping_out_t* out = (swim_ping_out_t*)data;
    hg_return_t hret;
    hret = hg_proc_uint64_t(proc, &out->responder_incarnation);
    if(hret != HG_SUCCESS) return hret;
    hret = hg_proc_hg_size_t(proc, &out->num_gossip_entries);
    if(hret != HG_SUCCESS) return hret;
    for(size_t i = 0; i < out->num_gossip_entries && i < SWIM_MAX_GOSSIP_ENTRIES; i++) {
        hret = hg_proc_swim_gossip_entry_t(proc, &out->gossip_entries[i]);
        if(hret != HG_SUCCESS) return hret;
    }
    return HG_SUCCESS;
}

/* Ping-Req RPC input/output */
typedef struct swim_ping_req_in {
    char*               target_address;
    uint16_t            target_provider_id;
    uint64_t            sender_incarnation;
    size_t              num_gossip_entries;
    swim_gossip_entry_t gossip_entries[SWIM_MAX_GOSSIP_ENTRIES];
} swim_ping_req_in_t;

typedef struct swim_ping_req_out {
    uint8_t             target_responded;
    uint64_t            target_incarnation;
    size_t              num_gossip_entries;
    swim_gossip_entry_t gossip_entries[SWIM_MAX_GOSSIP_ENTRIES];
} swim_ping_req_out_t;

static inline hg_return_t hg_proc_swim_ping_req_in_t(hg_proc_t proc, void* data) {
    swim_ping_req_in_t* in = (swim_ping_req_in_t*)data;
    hg_return_t hret;
    hret = hg_proc_hg_string_t(proc, &in->target_address);
    if(hret != HG_SUCCESS) return hret;
    hret = hg_proc_uint16_t(proc, &in->target_provider_id);
    if(hret != HG_SUCCESS) return hret;
    hret = hg_proc_uint64_t(proc, &in->sender_incarnation);
    if(hret != HG_SUCCESS) return hret;
    hret = hg_proc_hg_size_t(proc, &in->num_gossip_entries);
    if(hret != HG_SUCCESS) return hret;
    for(size_t i = 0; i < in->num_gossip_entries && i < SWIM_MAX_GOSSIP_ENTRIES; i++) {
        hret = hg_proc_swim_gossip_entry_t(proc, &in->gossip_entries[i]);
        if(hret != HG_SUCCESS) return hret;
    }
    return HG_SUCCESS;
}

static inline hg_return_t hg_proc_swim_ping_req_out_t(hg_proc_t proc, void* data) {
    swim_ping_req_out_t* out = (swim_ping_req_out_t*)data;
    hg_return_t hret;
    hret = hg_proc_uint8_t(proc, &out->target_responded);
    if(hret != HG_SUCCESS) return hret;
    hret = hg_proc_uint64_t(proc, &out->target_incarnation);
    if(hret != HG_SUCCESS) return hret;
    hret = hg_proc_hg_size_t(proc, &out->num_gossip_entries);
    if(hret != HG_SUCCESS) return hret;
    for(size_t i = 0; i < out->num_gossip_entries && i < SWIM_MAX_GOSSIP_ENTRIES; i++) {
        hret = hg_proc_swim_gossip_entry_t(proc, &out->gossip_entries[i]);
        if(hret != HG_SUCCESS) return hret;
    }
    return HG_SUCCESS;
}

/* Announce RPC input */
typedef struct swim_announce_in {
    uint8_t             type;  /* SWIM_GOSSIP_JOIN or SWIM_GOSSIP_LEAVE */
    char*               address;
    uint16_t            provider_id;
    uint64_t            incarnation;
    size_t              num_gossip_entries;
    swim_gossip_entry_t gossip_entries[SWIM_MAX_GOSSIP_ENTRIES];
} swim_announce_in_t;

static inline hg_return_t hg_proc_swim_announce_in_t(hg_proc_t proc, void* data) {
    swim_announce_in_t* in = (swim_announce_in_t*)data;
    hg_return_t hret;
    hret = hg_proc_uint8_t(proc, &in->type);
    if(hret != HG_SUCCESS) return hret;
    hret = hg_proc_hg_string_t(proc, &in->address);
    if(hret != HG_SUCCESS) return hret;
    hret = hg_proc_uint16_t(proc, &in->provider_id);
    if(hret != HG_SUCCESS) return hret;
    hret = hg_proc_uint64_t(proc, &in->incarnation);
    if(hret != HG_SUCCESS) return hret;
    hret = hg_proc_hg_size_t(proc, &in->num_gossip_entries);
    if(hret != HG_SUCCESS) return hret;
    for(size_t i = 0; i < in->num_gossip_entries && i < SWIM_MAX_GOSSIP_ENTRIES; i++) {
        hret = hg_proc_swim_gossip_entry_t(proc, &in->gossip_entries[i]);
        if(hret != HG_SUCCESS) return hret;
    }
    return HG_SUCCESS;
}

/* ============================================================================
 * PING RPC HANDLER
 * ============================================================================ */

static void ping_rpc_ult(hg_handle_t h);
static DECLARE_MARGO_RPC_HANDLER(ping_rpc_ult)

static void ping_rpc_ult(hg_handle_t h) {
    margo_instance_id mid = margo_hg_handle_get_instance(h);
    const struct hg_info* info = margo_get_info(h);
    swim_context* ctx = (swim_context*)margo_registered_data(mid, info->id);

    swim_ping_in_t in = {0};
    swim_ping_out_t out = {0};

    hg_return_t hret = margo_get_input(h, &in);
    if(hret != HG_SUCCESS) {
        margo_error(mid, "[flock/swim] Failed to get ping input");
        goto finish;
    }

    /* Process incoming gossip */
    process_gossip_entries(ctx, in.gossip_entries, in.num_gossip_entries);

    /* Prepare response */
    out.responder_incarnation = ctx->self_incarnation;

    /* Gather gossip to piggyback */
    swim_gossip_buffer_gather(ctx->gossip_buffer, SWIM_MAX_GOSSIP_ENTRIES,
                              out.gossip_entries, &out.num_gossip_entries);

    margo_free_input(h, &in);

finish:
    margo_respond(h, &out);
    margo_destroy(h);
}
static DEFINE_MARGO_RPC_HANDLER(ping_rpc_ult)

/* ============================================================================
 * PING-REQ RPC HANDLER
 * ============================================================================ */

static void ping_req_rpc_ult(hg_handle_t h);
static DECLARE_MARGO_RPC_HANDLER(ping_req_rpc_ult)

static void ping_req_rpc_ult(hg_handle_t h) {
    margo_instance_id mid = margo_hg_handle_get_instance(h);
    const struct hg_info* info = margo_get_info(h);
    swim_context* ctx = (swim_context*)margo_registered_data(mid, info->id);

    swim_ping_req_in_t in = {0};
    swim_ping_req_out_t out = {0};
    hg_handle_t ping_handle = HG_HANDLE_NULL;
    hg_addr_t target_addr = HG_ADDR_NULL;

    hg_return_t hret = margo_get_input(h, &in);
    if(hret != HG_SUCCESS) {
        margo_error(mid, "[flock/swim] Failed to get ping-req input");
        goto finish;
    }

    /* Process incoming gossip */
    process_gossip_entries(ctx, in.gossip_entries, in.num_gossip_entries);

    /* Look up target address */
    hret = margo_addr_lookup(mid, in.target_address, &target_addr);
    if(hret != HG_SUCCESS) {
        margo_warning(mid, "[flock/swim] Failed to lookup target address for ping-req");
        out.target_responded = 0;
        goto respond;
    }

    /* Create ping handle to target */
    hret = margo_create(mid, target_addr, ctx->ping_rpc_id, &ping_handle);
    if(hret != HG_SUCCESS) {
        margo_warning(mid, "[flock/swim] Failed to create ping handle for ping-req");
        out.target_responded = 0;
        goto respond;
    }

    /* Prepare ping input */
    swim_ping_in_t ping_in = {0};
    ping_in.sender_incarnation = ctx->self_incarnation;
    swim_gossip_buffer_gather(ctx->gossip_buffer, SWIM_MAX_GOSSIP_ENTRIES,
                              ping_in.gossip_entries, &ping_in.num_gossip_entries);

    /* Send ping with timeout */
    hret = margo_provider_forward_timed(in.target_provider_id, ping_handle, &ping_in,
                                        ctx->ping_timeout_ms);
    if(hret != HG_SUCCESS) {
        out.target_responded = 0;
        goto respond;
    }

    /* Get response */
    swim_ping_out_t ping_out = {0};
    hret = margo_get_output(ping_handle, &ping_out);
    if(hret != HG_SUCCESS) {
        out.target_responded = 0;
        goto respond;
    }

    /* Process gossip from target */
    process_gossip_entries(ctx, ping_out.gossip_entries, ping_out.num_gossip_entries);

    out.target_responded = 1;
    out.target_incarnation = ping_out.responder_incarnation;

    margo_free_output(ping_handle, &ping_out);

respond:
    /* Gather gossip to piggyback */
    swim_gossip_buffer_gather(ctx->gossip_buffer, SWIM_MAX_GOSSIP_ENTRIES,
                              out.gossip_entries, &out.num_gossip_entries);

    margo_free_input(h, &in);
    if(ping_handle) margo_destroy(ping_handle);
    if(target_addr != HG_ADDR_NULL) margo_addr_free(mid, target_addr);

finish:
    margo_respond(h, &out);
    margo_destroy(h);
}
static DEFINE_MARGO_RPC_HANDLER(ping_req_rpc_ult)

/* ============================================================================
 * ANNOUNCE RPC HANDLER
 * ============================================================================ */

static void announce_rpc_ult(hg_handle_t h);
static DECLARE_MARGO_RPC_HANDLER(announce_rpc_ult)

static void announce_rpc_ult(hg_handle_t h) {
    margo_instance_id mid = margo_hg_handle_get_instance(h);
    const struct hg_info* info = margo_get_info(h);
    swim_context* ctx = (swim_context*)margo_registered_data(mid, info->id);

    swim_announce_in_t in = {0};

    hg_return_t hret = margo_get_input(h, &in);
    if(hret != HG_SUCCESS) {
        margo_error(mid, "[flock/swim] Failed to get announce input");
        goto finish;
    }

    /* Process incoming gossip */
    process_gossip_entries(ctx, in.gossip_entries, in.num_gossip_entries);

    /* Process the announcement */
    swim_gossip_entry_t announce_entry = {
        .type = (swim_gossip_type_t)in.type,
        .address = in.address,
        .provider_id = in.provider_id,
        .incarnation = in.incarnation
    };
    process_gossip_entries(ctx, &announce_entry, 1);

    margo_free_input(h, &in);

finish:
    margo_respond(h, NULL);
    margo_destroy(h);
}
static DEFINE_MARGO_RPC_HANDLER(announce_rpc_ult)

/* ============================================================================
 * GOSSIP PROCESSING
 * ============================================================================ */

static void process_gossip_entries(swim_context* ctx, swim_gossip_entry_t* entries, size_t num_entries) {
    if(!entries || num_entries == 0) return;

    for(size_t i = 0; i < num_entries; i++) {
        swim_gossip_entry_t* entry = &entries[i];

        /* Check if this is about us */
        if(entry->provider_id == ctx->provider_id &&
           strcmp(entry->address, ctx->self_address) == 0) {
            if(entry->type == SWIM_GOSSIP_SUSPECT) {
                /* We're being suspected - refute! */
                refute_suspicion(ctx);
            }
            continue;
        }

        FLOCK_GROUP_VIEW_LOCK(&ctx->view);
        flock_member_t* member = flock_group_view_find_member(
            &ctx->view, entry->address, entry->provider_id);

        switch(entry->type) {
            case SWIM_GOSSIP_ALIVE: {
                if(!member) {
                    /* New member - add to view */
                    member = flock_group_view_add_member(&ctx->view, entry->address, entry->provider_id);
                    if(member) {
                        swim_member_state_t* state = (swim_member_state_t*)calloc(1, sizeof(*state));
                        if(state) {
                            state->context = ctx;
                            state->status = SWIM_ALIVE;
                            state->incarnation = entry->incarnation;
                            state->suspicion_start = 0;
                            margo_addr_lookup(ctx->mid, entry->address, &state->address);
                            member->extra.data = state;
                            member->extra.free = swim_member_state_free;
                        }
                        /* Update probe order */
                        shuffle_probe_order(ctx);
                        swim_gossip_buffer_set_group_size(ctx->gossip_buffer, ctx->view.members.size);

                        FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);
                        if(ctx->member_update_callback) {
                            ctx->member_update_callback(ctx->callback_context,
                                FLOCK_MEMBER_JOINED, entry->address, entry->provider_id);
                        }
                        goto next_entry;
                    }
                } else {
                    swim_member_state_t* state = get_member_state(member);
                    if(state && entry->incarnation > state->incarnation) {
                        state->status = SWIM_ALIVE;
                        state->incarnation = entry->incarnation;
                        state->suspicion_start = 0;
                    } else if(state && entry->incarnation == state->incarnation &&
                              state->status == SWIM_SUSPECTED) {
                        /* Same incarnation but ALIVE beats SUSPECT */
                        state->status = SWIM_ALIVE;
                        state->suspicion_start = 0;
                    }
                }
                break;
            }
            case SWIM_GOSSIP_SUSPECT: {
                if(member) {
                    swim_member_state_t* state = get_member_state(member);
                    if(state && entry->incarnation >= state->incarnation &&
                       state->status == SWIM_ALIVE) {
                        state->status = SWIM_SUSPECTED;
                        state->incarnation = entry->incarnation;
                        state->suspicion_start = ABT_get_wtime();
                        margo_debug(ctx->mid, "[flock/swim] Member (%s, %d) suspected",
                                   member->address, member->provider_id);
                    }
                }
                break;
            }
            case SWIM_GOSSIP_CONFIRM:
            case SWIM_GOSSIP_LEAVE: {
                if(member) {
                    char* addr_copy = strdup(member->address);
                    uint16_t pid = member->provider_id;
                    flock_update_t update_type = (entry->type == SWIM_GOSSIP_LEAVE) ?
                                                 FLOCK_MEMBER_LEFT : FLOCK_MEMBER_DIED;
                    flock_group_view_remove_member(&ctx->view, member);
                    shuffle_probe_order(ctx);
                    swim_gossip_buffer_set_group_size(ctx->gossip_buffer, ctx->view.members.size);

                    FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);
                    if(ctx->member_update_callback) {
                        ctx->member_update_callback(ctx->callback_context,
                            update_type, addr_copy, pid);
                    }
                    free(addr_copy);
                    goto next_entry;
                }
                break;
            }
            case SWIM_GOSSIP_JOIN: {
                if(!member) {
                    member = flock_group_view_add_member(&ctx->view, entry->address, entry->provider_id);
                    if(member) {
                        swim_member_state_t* state = (swim_member_state_t*)calloc(1, sizeof(*state));
                        if(state) {
                            state->context = ctx;
                            state->status = SWIM_ALIVE;
                            state->incarnation = entry->incarnation;
                            state->suspicion_start = 0;
                            margo_addr_lookup(ctx->mid, entry->address, &state->address);
                            member->extra.data = state;
                            member->extra.free = swim_member_state_free;
                        }
                        shuffle_probe_order(ctx);
                        swim_gossip_buffer_set_group_size(ctx->gossip_buffer, ctx->view.members.size);

                        FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);
                        if(ctx->member_update_callback) {
                            ctx->member_update_callback(ctx->callback_context,
                                FLOCK_MEMBER_JOINED, entry->address, entry->provider_id);
                        }
                        goto next_entry;
                    }
                }
                break;
            }
        }
        FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);

        /* Add to gossip buffer for further dissemination */
        swim_gossip_buffer_add(ctx->gossip_buffer, entry->type,
                               entry->address, entry->provider_id, entry->incarnation);
next_entry:;
    }
}

/* ============================================================================
 * PROTOCOL TIMER AND PROBING
 * ============================================================================ */

static void protocol_timer_callback(void* args) {
    swim_context* ctx = (swim_context*)args;

    if(atomic_load(&ctx->shutting_down)) return;

    /* Step 1: Check suspicion timeouts */
    check_suspicion_timeouts(ctx);

    /* Clean up old gossip entries */
    swim_gossip_buffer_cleanup(ctx->gossip_buffer);

    FLOCK_GROUP_VIEW_LOCK(&ctx->view);
    size_t num_members = ctx->view.members.size;

    /* Need at least 2 members (including self) to probe */
    if(num_members < 2) {
        FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);
        goto restart_timer;
    }

    /* Step 2: Select next probe target (round-robin through shuffled order) */
    size_t target_idx = SIZE_MAX;
    size_t attempts = 0;
    while(attempts < ctx->probe_order_size) {
        size_t idx = ctx->probe_order[ctx->probe_index];
        ctx->probe_index = (ctx->probe_index + 1) % ctx->probe_order_size;
        attempts++;

        if(idx >= num_members) continue;
        flock_member_t* member = &ctx->view.members.data[idx];
        /* Skip self */
        if(member->provider_id == ctx->provider_id &&
           strcmp(member->address, ctx->self_address) == 0) continue;
        /* Skip confirmed dead members */
        swim_member_state_t* state = get_member_state(member);
        if(state && state->status == SWIM_CONFIRMED_DEAD) continue;

        target_idx = idx;
        break;
    }

    if(target_idx == SIZE_MAX) {
        FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);
        goto restart_timer;
    }

    flock_member_t* target = &ctx->view.members.data[target_idx];
    swim_member_state_t* target_state = get_member_state(target);
    if(!target_state || target_state->address == HG_ADDR_NULL) {
        FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);
        goto restart_timer;
    }

    char* target_address = strdup(target->address);
    uint16_t target_provider_id = target->provider_id;
    hg_addr_t target_hg_addr = target_state->address;
    FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);

    /* Step 3: Send direct ping */
    hg_handle_t ping_handle = HG_HANDLE_NULL;
    hg_return_t hret = margo_create(ctx->mid, target_hg_addr, ctx->ping_rpc_id, &ping_handle);
    if(hret != HG_SUCCESS) {
        margo_warning(ctx->mid, "[flock/swim] Failed to create ping handle");
        free(target_address);
        goto restart_timer;
    }

    swim_ping_in_t ping_in = {0};
    ping_in.sender_incarnation = ctx->self_incarnation;
    swim_gossip_buffer_gather(ctx->gossip_buffer, SWIM_MAX_GOSSIP_ENTRIES,
                              ping_in.gossip_entries, &ping_in.num_gossip_entries);

    hret = margo_provider_forward_timed(target_provider_id, ping_handle, &ping_in,
                                        ctx->ping_timeout_ms);

    bool ping_succeeded = false;
    if(hret == HG_SUCCESS) {
        swim_ping_out_t ping_out = {0};
        hret = margo_get_output(ping_handle, &ping_out);
        if(hret == HG_SUCCESS) {
            ping_succeeded = true;
            process_gossip_entries(ctx, ping_out.gossip_entries, ping_out.num_gossip_entries);

            /* Update member state to ALIVE if it was suspected */
            FLOCK_GROUP_VIEW_LOCK(&ctx->view);
            flock_member_t* member = flock_group_view_find_member(
                &ctx->view, target_address, target_provider_id);
            if(member) {
                swim_member_state_t* state = get_member_state(member);
                if(state && state->status == SWIM_SUSPECTED) {
                    if(ping_out.responder_incarnation > state->incarnation) {
                        state->status = SWIM_ALIVE;
                        state->incarnation = ping_out.responder_incarnation;
                        state->suspicion_start = 0;
                        /* Disseminate ALIVE */
                        swim_gossip_buffer_add(ctx->gossip_buffer, SWIM_GOSSIP_ALIVE,
                                               target_address, target_provider_id,
                                               ping_out.responder_incarnation);
                    }
                }
            }
            FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);
            margo_free_output(ping_handle, &ping_out);
        }
    }
    margo_destroy(ping_handle);

    /* Step 4: If direct ping failed, try indirect probing */
    if(!ping_succeeded) {
        margo_debug(ctx->mid, "[flock/swim] Direct ping to (%s, %d) failed, trying indirect",
                   target_address, target_provider_id);

        size_t indirect_indices[8];
        FLOCK_GROUP_VIEW_LOCK(&ctx->view);
        size_t num_indirect = select_random_members(ctx, target_idx, indirect_indices,
                                                    ctx->ping_req_members);
        FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);

        bool any_indirect_succeeded = false;
        for(size_t i = 0; i < num_indirect && !any_indirect_succeeded; i++) {
            FLOCK_GROUP_VIEW_LOCK(&ctx->view);
            if(indirect_indices[i] >= ctx->view.members.size) {
                FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);
                continue;
            }
            flock_member_t* indirect = &ctx->view.members.data[indirect_indices[i]];
            swim_member_state_t* indirect_state = get_member_state(indirect);
            if(!indirect_state || indirect_state->address == HG_ADDR_NULL) {
                FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);
                continue;
            }

            uint16_t indirect_pid = indirect->provider_id;
            hg_addr_t indirect_addr = indirect_state->address;
            FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);

            hg_handle_t ping_req_handle = HG_HANDLE_NULL;
            hret = margo_create(ctx->mid, indirect_addr, ctx->ping_req_rpc_id, &ping_req_handle);
            if(hret != HG_SUCCESS) continue;

            swim_ping_req_in_t ping_req_in = {0};
            ping_req_in.target_address = target_address;
            ping_req_in.target_provider_id = target_provider_id;
            ping_req_in.sender_incarnation = ctx->self_incarnation;
            swim_gossip_buffer_gather(ctx->gossip_buffer, SWIM_MAX_GOSSIP_ENTRIES,
                                      ping_req_in.gossip_entries, &ping_req_in.num_gossip_entries);

            hret = margo_provider_forward_timed(indirect_pid, ping_req_handle, &ping_req_in,
                                                ctx->ping_req_timeout_ms);
            if(hret == HG_SUCCESS) {
                swim_ping_req_out_t ping_req_out = {0};
                hret = margo_get_output(ping_req_handle, &ping_req_out);
                if(hret == HG_SUCCESS) {
                    process_gossip_entries(ctx, ping_req_out.gossip_entries,
                                           ping_req_out.num_gossip_entries);
                    if(ping_req_out.target_responded) {
                        any_indirect_succeeded = true;
                        /* Update member state */
                        FLOCK_GROUP_VIEW_LOCK(&ctx->view);
                        flock_member_t* member = flock_group_view_find_member(
                            &ctx->view, target_address, target_provider_id);
                        if(member) {
                            swim_member_state_t* state = get_member_state(member);
                            if(state && state->status == SWIM_SUSPECTED &&
                               ping_req_out.target_incarnation > state->incarnation) {
                                state->status = SWIM_ALIVE;
                                state->incarnation = ping_req_out.target_incarnation;
                                state->suspicion_start = 0;
                            }
                        }
                        FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);
                    }
                    margo_free_output(ping_req_handle, &ping_req_out);
                }
            }
            margo_destroy(ping_req_handle);
        }

        /* Step 5: If still no response, mark as suspected */
        if(!any_indirect_succeeded) {
            FLOCK_GROUP_VIEW_LOCK(&ctx->view);
            ssize_t idx = flock_group_view_members_binary_search(
                &ctx->view, target_address, target_provider_id);
            if(idx >= 0) {
                mark_member_suspected(ctx, (size_t)idx);
            }
            FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);
        }
    }

    free(target_address);

restart_timer:
    if(!atomic_load(&ctx->shutting_down)) {
        margo_timer_start(ctx->protocol_timer, ctx->protocol_period_ms);
    }
}

/* ============================================================================
 * HELPER FUNCTIONS
 * ============================================================================ */

static void shuffle_probe_order(swim_context* ctx) {
    size_t n = ctx->view.members.size;
    if(n == 0) {
        free(ctx->probe_order);
        ctx->probe_order = NULL;
        ctx->probe_order_size = 0;
        ctx->probe_index = 0;
        return;
    }

    size_t* new_order = (size_t*)realloc(ctx->probe_order, n * sizeof(size_t));
    if(!new_order) return;
    ctx->probe_order = new_order;
    ctx->probe_order_size = n;

    /* Initialize with sequential indices */
    for(size_t i = 0; i < n; i++) {
        ctx->probe_order[i] = i;
    }

    /* Fisher-Yates shuffle */
    for(size_t i = n - 1; i > 0; i--) {
        size_t j = rand() % (i + 1);
        size_t tmp = ctx->probe_order[i];
        ctx->probe_order[i] = ctx->probe_order[j];
        ctx->probe_order[j] = tmp;
    }

    ctx->probe_index = 0;
}

static size_t select_random_members(swim_context* ctx, size_t exclude_idx, size_t* indices, size_t count) {
    /* Must be called with view locked */
    size_t n = ctx->view.members.size;
    if(n <= 2) return 0; /* Only self and target */

    /* Build list of valid indices (excluding self and target) */
    size_t* valid = (size_t*)malloc(n * sizeof(size_t));
    if(!valid) return 0;

    size_t valid_count = 0;
    for(size_t i = 0; i < n; i++) {
        if(i == exclude_idx) continue;
        flock_member_t* member = &ctx->view.members.data[i];
        if(member->provider_id == ctx->provider_id &&
           strcmp(member->address, ctx->self_address) == 0) continue;
        swim_member_state_t* state = get_member_state(member);
        if(state && state->status == SWIM_CONFIRMED_DEAD) continue;
        valid[valid_count++] = i;
    }

    /* Select random members */
    size_t selected = 0;
    while(selected < count && selected < valid_count) {
        size_t r = rand() % valid_count;
        indices[selected++] = valid[r];
        /* Remove from valid list */
        valid[r] = valid[--valid_count];
    }

    free(valid);
    return selected;
}

static void mark_member_suspected(swim_context* ctx, size_t member_idx) {
    /* Must be called with view locked */
    if(member_idx >= ctx->view.members.size) return;
    flock_member_t* member = &ctx->view.members.data[member_idx];
    swim_member_state_t* state = get_member_state(member);
    if(!state) return;

    if(state->status == SWIM_ALIVE) {
        state->status = SWIM_SUSPECTED;
        state->suspicion_start = ABT_get_wtime();
        margo_debug(ctx->mid, "[flock/swim] Marking member (%s, %d) as suspected",
                   member->address, member->provider_id);

        /* Add to gossip buffer */
        swim_gossip_buffer_add(ctx->gossip_buffer, SWIM_GOSSIP_SUSPECT,
                               member->address, member->provider_id, state->incarnation);
    }
}

static void mark_member_dead(swim_context* ctx, size_t member_idx) {
    /* Must be called with view locked */
    if(member_idx >= ctx->view.members.size) return;
    flock_member_t* member = &ctx->view.members.data[member_idx];

    char* address = strdup(member->address);
    uint16_t provider_id = member->provider_id;
    swim_member_state_t* state = get_member_state(member);
    uint64_t incarnation = state ? state->incarnation : 0;

    margo_debug(ctx->mid, "[flock/swim] Confirming member (%s, %d) as dead",
               address, provider_id);

    /* Add CONFIRM to gossip buffer before removing */
    swim_gossip_buffer_add(ctx->gossip_buffer, SWIM_GOSSIP_CONFIRM,
                           address, provider_id, incarnation);

    flock_group_view_remove_member(&ctx->view, member);
    shuffle_probe_order(ctx);
    swim_gossip_buffer_set_group_size(ctx->gossip_buffer, ctx->view.members.size);

    FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);

    if(ctx->member_update_callback) {
        ctx->member_update_callback(ctx->callback_context, FLOCK_MEMBER_DIED,
                                    address, provider_id);
    }

    FLOCK_GROUP_VIEW_LOCK(&ctx->view);
    free(address);
}

static void check_suspicion_timeouts(swim_context* ctx) {
    double now = ABT_get_wtime();
    double timeout_sec = ctx->suspicion_timeout_ms / 1000.0;

    FLOCK_GROUP_VIEW_LOCK(&ctx->view);
    for(size_t i = 0; i < ctx->view.members.size; ) {
        flock_member_t* member = &ctx->view.members.data[i];
        swim_member_state_t* state = get_member_state(member);

        if(state && state->status == SWIM_SUSPECTED) {
            if(now - state->suspicion_start >= timeout_sec) {
                mark_member_dead(ctx, i);
                /* mark_member_dead removes the member, so don't increment i */
                continue;
            }
        }
        i++;
    }
    FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);
}

static void refute_suspicion(swim_context* ctx) {
    ctx->self_incarnation++;
    margo_debug(ctx->mid, "[flock/swim] Refuting suspicion, new incarnation: %lu",
               (unsigned long)ctx->self_incarnation);
    swim_gossip_buffer_add(ctx->gossip_buffer, SWIM_GOSSIP_ALIVE,
                           ctx->self_address, ctx->provider_id, ctx->self_incarnation);
}

/* ============================================================================
 * JOIN/LEAVE
 * ============================================================================ */

static flock_return_t swim_announce_to_random_members(swim_context* ctx, swim_gossip_type_t type) {
    /* Select random members to announce to */
    FLOCK_GROUP_VIEW_LOCK(&ctx->view);
    size_t n = ctx->view.members.size;
    if(n == 0) {
        FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);
        return FLOCK_SUCCESS;
    }

    /* Announce to log(n)*2 members for good coverage */
    unsigned max_gossip = swim_compute_max_gossip(n);
    unsigned num_targets = max_gossip < n ? max_gossip : (unsigned)n;
    if(num_targets == 0) num_targets = 1;

    /* Collect member info while holding lock */
    typedef struct {
        hg_addr_t addr;
        uint16_t provider_id;
    } announce_target_t;

    announce_target_t* targets = (announce_target_t*)calloc(num_targets, sizeof(announce_target_t));
    if(!targets) {
        FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);
        return FLOCK_ERR_ALLOCATION;
    }

    size_t targets_collected = 0;
    for(size_t i = 0; i < n && targets_collected < num_targets; i++) {
        flock_member_t* member = &ctx->view.members.data[i];
        /* Skip self */
        if(member->provider_id == ctx->provider_id &&
           strcmp(member->address, ctx->self_address) == 0) continue;

        swim_member_state_t* state = get_member_state(member);
        if(!state || state->address == HG_ADDR_NULL) continue;

        targets[targets_collected].addr = state->address;
        targets[targets_collected].provider_id = member->provider_id;
        targets_collected++;
    }
    FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);

    /* Send announcements */
    for(size_t i = 0; i < targets_collected; i++) {
        hg_handle_t handle = HG_HANDLE_NULL;
        hg_return_t hret = margo_create(ctx->mid, targets[i].addr, ctx->announce_rpc_id, &handle);
        if(hret != HG_SUCCESS) continue;

        swim_announce_in_t in = {0};
        in.type = (uint8_t)type;
        in.address = ctx->self_address;
        in.provider_id = ctx->provider_id;
        in.incarnation = ctx->self_incarnation;
        swim_gossip_buffer_gather(ctx->gossip_buffer, SWIM_MAX_GOSSIP_ENTRIES,
                                  in.gossip_entries, &in.num_gossip_entries);

        margo_provider_forward_timed(targets[i].provider_id, handle, &in, 1000.0);
        margo_destroy(handle);
    }

    free(targets);
    return FLOCK_SUCCESS;
}

/* ============================================================================
 * BACKEND INTERFACE IMPLEMENTATION
 * ============================================================================ */

static flock_return_t swim_create_group(
    flock_backend_init_args_t* args,
    void** context)
{
    flock_return_t ret = FLOCK_SUCCESS;
    swim_context* ctx = NULL;

    /* Parse configuration */
    double protocol_period_ms = 1000.0;
    double ping_timeout_ms = 200.0;
    double ping_req_timeout_ms = 500.0;
    unsigned ping_req_members = 3;
    double suspicion_timeout_ms = 5000.0;

    if(args->config) {
        struct json_object* val;
        if((val = json_object_object_get(args->config, "protocol_period_ms"))) {
            if(json_object_is_type(val, json_type_double) || json_object_is_type(val, json_type_int))
                protocol_period_ms = json_object_get_double(val);
        }
        if((val = json_object_object_get(args->config, "ping_timeout_ms"))) {
            if(json_object_is_type(val, json_type_double) || json_object_is_type(val, json_type_int))
                ping_timeout_ms = json_object_get_double(val);
        }
        if((val = json_object_object_get(args->config, "ping_req_timeout_ms"))) {
            if(json_object_is_type(val, json_type_double) || json_object_is_type(val, json_type_int))
                ping_req_timeout_ms = json_object_get_double(val);
        }
        if((val = json_object_object_get(args->config, "ping_req_members"))) {
            if(json_object_is_type(val, json_type_int))
                ping_req_members = (unsigned)json_object_get_int(val);
        }
        if((val = json_object_object_get(args->config, "suspicion_timeout_ms"))) {
            if(json_object_is_type(val, json_type_double) || json_object_is_type(val, json_type_int))
                suspicion_timeout_ms = json_object_get_double(val);
        }
    }

    /* Get self address */
    margo_instance_id mid = args->mid;
    hg_addr_t self_address;
    char self_address_string[256];
    hg_size_t self_address_size = 256;
    hg_return_t hret = margo_addr_self(mid, &self_address);
    if(hret != HG_SUCCESS) {
        return FLOCK_ERR_FROM_MERCURY;
    }
    hret = margo_addr_to_string(mid, self_address_string, &self_address_size, self_address);
    margo_addr_free(mid, self_address);
    if(hret != HG_SUCCESS) {
        return FLOCK_ERR_FROM_MERCURY;
    }

    /* Allocate context */
    ctx = (swim_context*)calloc(1, sizeof(*ctx));
    if(!ctx) {
        return FLOCK_ERR_ALLOCATION;
    }

    ctx->mid = mid;
    ctx->provider_id = args->provider_id;
    ctx->pool = args->pool;
    ctx->self_address = strdup(self_address_string);
    ctx->self_incarnation = 1;
    atomic_store(&ctx->shutting_down, false);

    ctx->protocol_period_ms = protocol_period_ms;
    ctx->ping_timeout_ms = ping_timeout_ms;
    ctx->ping_req_timeout_ms = ping_req_timeout_ms;
    ctx->ping_req_members = ping_req_members;
    ctx->suspicion_timeout_ms = suspicion_timeout_ms;

    ctx->member_update_callback = args->member_update_callback;
    ctx->metadata_update_callback = args->metadata_update_callback;
    ctx->callback_context = args->callback_context;

    /* Create config object */
    ctx->config = json_object_new_object();
    json_object_object_add(ctx->config, "protocol_period_ms",
                           json_object_new_double(protocol_period_ms));
    json_object_object_add(ctx->config, "ping_timeout_ms",
                           json_object_new_double(ping_timeout_ms));
    json_object_object_add(ctx->config, "ping_req_timeout_ms",
                           json_object_new_double(ping_req_timeout_ms));
    json_object_object_add(ctx->config, "ping_req_members",
                           json_object_new_int(ping_req_members));
    json_object_object_add(ctx->config, "suspicion_timeout_ms",
                           json_object_new_double(suspicion_timeout_ms));

    /* Create gossip buffer */
    ret = swim_gossip_buffer_create(&ctx->gossip_buffer);
    if(ret != FLOCK_SUCCESS) {
        goto error;
    }

    /* Move initial view */
    FLOCK_GROUP_VIEW_MOVE(&args->initial_view, &ctx->view);

    /* Add metadata */
    flock_group_view_add_metadata(&ctx->view, "__config__",
        json_object_to_json_string_ext(ctx->config, JSON_C_TO_STRING_NOSLASHESCAPE));
    flock_group_view_add_metadata(&ctx->view, "__type__", "swim");

    /* Initialize member states */
    for(size_t i = 0; i < ctx->view.members.size; i++) {
        flock_member_t* member = &ctx->view.members.data[i];
        swim_member_state_t* state = (swim_member_state_t*)calloc(1, sizeof(*state));
        if(!state) {
            ret = FLOCK_ERR_ALLOCATION;
            goto error;
        }
        state->context = ctx;
        state->status = SWIM_ALIVE;
        state->incarnation = 1;
        state->suspicion_start = 0;
        hret = margo_addr_lookup(mid, member->address, &state->address);
        if(hret != HG_SUCCESS) {
            free(state);
            ret = FLOCK_ERR_FROM_MERCURY;
            goto error;
        }
        member->extra.data = state;
        member->extra.free = swim_member_state_free;
    }

    swim_gossip_buffer_set_group_size(ctx->gossip_buffer, ctx->view.members.size);
    shuffle_probe_order(ctx);

    /* Register RPCs */
    ctx->ping_rpc_id = MARGO_REGISTER_PROVIDER(
        mid, "flock_swim_ping",
        swim_ping_in_t, swim_ping_out_t, ping_rpc_ult,
        args->provider_id, args->pool);
    margo_register_data(mid, ctx->ping_rpc_id, ctx, NULL);

    ctx->ping_req_rpc_id = MARGO_REGISTER_PROVIDER(
        mid, "flock_swim_ping_req",
        swim_ping_req_in_t, swim_ping_req_out_t, ping_req_rpc_ult,
        args->provider_id, args->pool);
    margo_register_data(mid, ctx->ping_req_rpc_id, ctx, NULL);

    ctx->announce_rpc_id = MARGO_REGISTER_PROVIDER(
        mid, "flock_swim_announce",
        swim_announce_in_t, void, announce_rpc_ult,
        args->provider_id, args->pool);
    margo_register_data(mid, ctx->announce_rpc_id, ctx, NULL);

    /* If joining an existing group, announce join */
    if(args->join) {
        /* Add ourselves to the view if not already there */
        if(!flock_group_view_find_member(&ctx->view, ctx->self_address, ctx->provider_id)) {
            flock_member_t* self_member = flock_group_view_add_member(
                &ctx->view, ctx->self_address, ctx->provider_id);
            if(self_member) {
                swim_member_state_t* state = (swim_member_state_t*)calloc(1, sizeof(*state));
                if(state) {
                    state->context = ctx;
                    state->status = SWIM_ALIVE;
                    state->incarnation = ctx->self_incarnation;
                    state->suspicion_start = 0;
                    margo_addr_self(mid, &state->address);
                    self_member->extra.data = state;
                    self_member->extra.free = swim_member_state_free;
                }
            }
            shuffle_probe_order(ctx);
        }

        swim_gossip_buffer_add(ctx->gossip_buffer, SWIM_GOSSIP_JOIN,
                               ctx->self_address, ctx->provider_id, ctx->self_incarnation);
        swim_announce_to_random_members(ctx, SWIM_GOSSIP_JOIN);
    }

    /* Create and start protocol timer */
    margo_timer_create(mid, protocol_timer_callback, ctx, &ctx->protocol_timer);
    margo_timer_start(ctx->protocol_timer, ctx->protocol_period_ms);

    *context = ctx;
    return FLOCK_SUCCESS;

error:
    swim_destroy_group(ctx);
    return ret;
}

static flock_return_t swim_destroy_group(void* ctx_ptr) {
    if(!ctx_ptr) return FLOCK_SUCCESS;
    swim_context* ctx = (swim_context*)ctx_ptr;

    atomic_store(&ctx->shutting_down, true);

    /* Announce leave to other members */
    if(ctx->view.members.size > 1) {
        swim_gossip_buffer_add(ctx->gossip_buffer, SWIM_GOSSIP_LEAVE,
                               ctx->self_address, ctx->provider_id, ctx->self_incarnation);
        swim_announce_to_random_members(ctx, SWIM_GOSSIP_LEAVE);
    }

    /* Cancel and destroy timer */
    if(ctx->protocol_timer) {
        margo_timer_cancel(ctx->protocol_timer);
        margo_timer_destroy(ctx->protocol_timer);
    }

    /* Free member addresses */
    FLOCK_GROUP_VIEW_LOCK(&ctx->view);
    for(size_t i = 0; i < ctx->view.members.size; i++) {
        swim_member_state_t* state = get_member_state(&ctx->view.members.data[i]);
        if(state && state->address != HG_ADDR_NULL) {
            margo_addr_free(ctx->mid, state->address);
            state->address = HG_ADDR_NULL;
        }
    }
    FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);

    /* Deregister RPCs */
    if(ctx->ping_rpc_id) margo_deregister(ctx->mid, ctx->ping_rpc_id);
    if(ctx->ping_req_rpc_id) margo_deregister(ctx->mid, ctx->ping_req_rpc_id);
    if(ctx->announce_rpc_id) margo_deregister(ctx->mid, ctx->announce_rpc_id);

    /* Free resources */
    if(ctx->gossip_buffer) swim_gossip_buffer_destroy(ctx->gossip_buffer);
    if(ctx->config) json_object_put(ctx->config);
    flock_group_view_clear(&ctx->view);
    free(ctx->probe_order);
    free(ctx->self_address);
    free(ctx);

    return FLOCK_SUCCESS;
}

static flock_return_t swim_get_config(
    void* ctx_ptr, void (*fn)(void*, const struct json_object*), void* uargs)
{
    swim_context* ctx = (swim_context*)ctx_ptr;
    ABT_mutex_lock(ABT_MUTEX_MEMORY_GET_HANDLE(&ctx->config_mtx));
    fn(uargs, ctx->config);
    ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&ctx->config_mtx));
    return FLOCK_SUCCESS;
}

static flock_return_t swim_get_view(
    void* ctx_ptr, void (*fn)(void*, const flock_group_view_t*), void* uargs)
{
    swim_context* ctx = (swim_context*)ctx_ptr;
    fn(uargs, &ctx->view);
    return FLOCK_SUCCESS;
}

static flock_return_t swim_add_metadata(void* ctx, const char* key, const char* value) {
    (void)ctx;
    (void)key;
    (void)value;
    return FLOCK_ERR_OP_UNSUPPORTED;
}

static flock_return_t swim_remove_metadata(void* ctx, const char* key) {
    (void)ctx;
    (void)key;
    return FLOCK_ERR_OP_UNSUPPORTED;
}

static flock_backend_impl swim_backend = {
    .name            = "swim",
    .init_group      = swim_create_group,
    .destroy_group   = swim_destroy_group,
    .get_config      = swim_get_config,
    .get_view        = swim_get_view,
    .add_metadata    = swim_add_metadata,
    .remove_metadata = swim_remove_metadata
};

flock_return_t flock_register_swim_backend(void) {
    return flock_register_backend(&swim_backend);
}
