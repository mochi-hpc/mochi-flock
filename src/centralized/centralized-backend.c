/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <string.h>
#include <stdlib.h>
#include <json-c/json.h>
#include <margo-timer.h>
#include "flock/flock-backend.h"
#include "flock/flock-group-view.h"
#include "../provider.h"
#include "centralized-backend.h"

#define RAND_BETWEEN(x, y) ((x) + (((double)rand()) / RAND_MAX)*(y-x))

/**
 * @brief The "centralized" backend uses the member with rank 0
 * as a centralized authority that is supposed to hold the most up to date
 * group view. The flock_group_view_t in all the other processes is a
 * read-only, cached version. The primary member will ping the secondary
 * members periodically to check that they are alive.
 */
typedef struct centralized_context {
    margo_instance_id    mid;
    uint64_t             rank;
    struct json_object*  config;
    ABT_mutex_memory     config_mtx;
    bool                 is_primary;
    struct {
        hg_addr_t        address;
        uint16_t         provider_id;
    } primary;
    flock_group_view_t   view;
    // RPCs sent by rank 0 to other ranks
    hg_id_t              ping_rpc_id;
    hg_id_t              membership_update_rpc_id;
    // RPCs sent by other ranks to rank 0
    hg_id_t              get_view_rpc_id;
    hg_id_t              leave_rpc_id;
    /* extracted from configuration */
    double ping_timeout_ms;
    double ping_interval_ms_min;
    double ping_interval_ms_max;
    unsigned ping_max_num_timeouts;
    /* update callbacks */
    flock_membership_update_fn member_update_callback;
    flock_metadata_update_fn   metadata_update_callback;
    void*                      callback_context;
} centralized_context;

/**
 * @brief Extra data attached to each member in the view in the primary member.
 */
typedef struct member_state {
    flock_member_t*      owner;
    centralized_context* context;
    hg_addr_t            address;
    margo_timer_t        ping_timer;
    _Atomic bool         in_timer_callback;
    double               last_ping_timestamp;
    hg_handle_t          last_ping_handle;
    uint8_t              num_ping_timeouts;
    ABT_mutex_memory     mtx;
} member_state;

static inline void member_state_free(void* args)
{
    member_state* state = (member_state*)args;
    if(!state) return;
    ABT_mutex_lock(ABT_MUTEX_MEMORY_GET_HANDLE(&state->mtx));
    if(state->last_ping_handle) {
        HG_Cancel(state->last_ping_handle);
    }
    if(state->ping_timer) {
        if(!state->in_timer_callback)
            margo_timer_cancel(state->ping_timer);
        margo_timer_destroy(state->ping_timer);
        state->ping_timer = MARGO_TIMER_NULL;
    }
    margo_addr_free(state->context->mid, state->address);
    ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&state->mtx));
    free(state);
}

/**
 * Ping RPC declaration.
 */
static DECLARE_MARGO_RPC_HANDLER(ping_rpc_ult)
static void ping_rpc_ult(hg_handle_t h);

/**
 * Callback periodically called by the timer setup
 * by rank 0 to ping other members.
 */
static void ping_timer_callback(void* args);

/**
 * Membership update RPC.
 */
static flock_return_t get_view(centralized_context* ctx);
static DECLARE_MARGO_RPC_HANDLER(get_view_rpc_ult)
static void get_view_rpc_ult(hg_handle_t h);

/**
 * Membership update RPC declaration and types.
 */
MERCURY_GEN_PROC(membership_update_in_t,
    ((uint8_t)(update))\
    ((uint64_t)(rank))\
    ((hg_const_string_t)(address))\
    ((uint16_t)(provider_id)))

MERCURY_GEN_PROC(membership_update_out_t,
    ((uint32_t)(ret)))

static flock_return_t broadcast_membership_update(
    centralized_context* ctx, flock_update_t update,
    size_t rank, const char* address, uint16_t provider_id);
static DECLARE_MARGO_RPC_HANDLER(membership_update_rpc_ult)
static void membership_update_rpc_ult(hg_handle_t h);

/**
 * Member leaving RPC declaration.
 */
static flock_return_t leave(centralized_context* ctx);
static DECLARE_MARGO_RPC_HANDLER(leave_rpc_ult)
static void leave_rpc_ult(hg_handle_t h);

/**
 * Forward-declaration of the function that destroys the group.
 */
static flock_return_t centralized_destroy_group(void* ctx);

/**
 * The configuration for a centralized group may look like the following:
 *
 * ```
 * {
 *    "ping_timeout_ms": X,
 *    "ping_interval_ms": Y or [Ymin, Ymax],
 *    "ping_max_num_timeouts": Z
 * }
 * ```
 *
 * - ping_timeout_ms is the timeout value when sending a ping RPC to a member.
 * - ping_interval_ms is the time to wait between two ping RPCs to the same follower.
 * - ping_max_num_timeouts is the number of RPC timeouts before the member is considered dead.
 * ping_interval_ms may be a list of two values [Ymin,Ymax] instead of a single value Y. If a list
 * is provided, the interval will be drawn randomly from a uniform distribution in the [Ymin,Ymax]
 * range each time.
 */
static flock_return_t centralized_create_group(
        flock_backend_init_args_t* args,
        void** context)
{
    flock_return_t ret = FLOCK_SUCCESS;

    // check that the initial view has a rank 0
    if(args->initial_view.members.size == 0
    || args->initial_view.members.data[0].rank != 0) {
        margo_error(args->mid, "[flock] Rank 0 not found for centralized backend");
        return FLOCK_ERR_INVALID_ARGS;
    }

    double   ping_timeout_ms_val   = 1000.0;
    double   ping_interval_ms_min  = 1000.0;
    double   ping_interval_ms_max  = 1000.0;
    unsigned ping_max_num_timeouts = 3;

    if(args->config) {
        if(!json_object_is_type(args->config, json_type_object)) {
            margo_error(args->mid,
                "[flock] Invalid configuration type for centralized backend (expected object)");
            return FLOCK_ERR_INVALID_CONFIG;
        }
        // process "ping_timeout_ms"
        struct json_object* ping_timeout_ms =
            json_object_object_get(args->config, "ping_timeout_ms");
        if(ping_timeout_ms) {
            if(json_object_is_type(ping_timeout_ms, json_type_double)) {
                ping_timeout_ms_val = json_object_get_double(ping_timeout_ms);
            } else {
                margo_error(args->mid,
                        "[flock] In centralized backend configuration: "
                        "\"ping_timeout_ms\" should be a number");
                return FLOCK_ERR_INVALID_CONFIG;
            }
            if(ping_timeout_ms_val < 0) {
                margo_error(args->mid,
                        "[flock] In centralized backend configuration: "
                        "\"ping_timeout_ms\" should be positive");
                return FLOCK_ERR_INVALID_CONFIG;
            }
        }
        // process "ping_interval_ms"
        struct json_object* ping_interval_ms_pair =
            json_object_object_get(args->config, "ping_interval_ms");
        if(ping_interval_ms_pair) {
            if(json_object_is_type(ping_interval_ms_pair, json_type_double)) {
                // ping_interval_ms is a single number
                ping_interval_ms_min = ping_interval_ms_max = json_object_get_double(ping_interval_ms_pair);
            } else if(json_object_is_type(ping_interval_ms_pair, json_type_array)
            && json_object_array_length(ping_interval_ms_pair) == 2) {
                // ping_timeout_ms is an array of two numbers (min and max)
                struct json_object* a = json_object_array_get_idx(ping_interval_ms_pair, 0);
                struct json_object* b = json_object_array_get_idx(ping_interval_ms_pair, 1);
                if(!(json_object_is_type(a, json_type_double) && json_object_is_type(b, json_type_double))) {
                    margo_error(args->mid,
                        "[flock] In centralized backend configuration: "
                        "\"ping_interval_ms\" should be an array of two numbers");
                    return FLOCK_ERR_INVALID_CONFIG;
                }
                ping_interval_ms_min = json_object_get_double(a);
                ping_interval_ms_max = json_object_get_double(b);
                if(ping_interval_ms_min > ping_interval_ms_max
                || ping_interval_ms_min < 0 || ping_interval_ms_max < 0) {
                    margo_error(args->mid,
                        "[flock] In centralized backend configuration: "
                        "invalid values or order in \"ping_interval_ms\" array");
                    return FLOCK_ERR_INVALID_CONFIG;
                }
            }
        }
        // process ping_max_num_timeouts
        struct json_object* ping_max_num_timeouts_json =
            json_object_object_get(args->config, "ping_max_num_timeouts");
        if(ping_max_num_timeouts_json) {
            if(!json_object_is_type(ping_max_num_timeouts_json, json_type_int)) {
                margo_error(args->mid,
                    "[flock] In centralized backend configuration: "
                    "\"ping_max_num_timeouts\" should be an integer");
                return FLOCK_ERR_INVALID_CONFIG;
            }
            int x = json_object_get_int(ping_max_num_timeouts_json);
            if(x < 1) {
                margo_error(args->mid,
                        "[flock] In centralized backend configuration: "
                        "\"ping_max_num_timeouts\" should be > 1");
                return FLOCK_ERR_INVALID_CONFIG;
            }
            ping_max_num_timeouts = (unsigned)x;
        }
    }

    // fill a json_object structure with the final configuration
    struct json_object* config = json_object_new_object();
    json_object_object_add(config, "ping_timeout_ms",
        json_object_new_double(ping_timeout_ms_val));
    if(ping_interval_ms_min != ping_interval_ms_max) {
        struct json_object* ping_interval_ms_pair = json_object_new_array_ext(2);
        json_object_array_add(ping_interval_ms_pair, json_object_new_double(ping_interval_ms_min));
        json_object_array_add(ping_interval_ms_pair, json_object_new_double(ping_interval_ms_max));
        json_object_object_add(config, "ping_interval_ms", ping_interval_ms_pair);
    } else {
        json_object_object_add(config, "ping_interval_ms",
                json_object_new_double(ping_interval_ms_min));
    }
    json_object_object_add(config, "ping_max_num_timeouts",
        json_object_new_uint64(ping_max_num_timeouts));

    /* group must have at least one member */
    if(args->initial_view.members.size == 0)
        return FLOCK_ERR_INVALID_ARGS;

    /* allocate context */
    centralized_context* ctx = (centralized_context*)calloc(1, sizeof(*ctx));
    if(!ctx) return FLOCK_ERR_ALLOCATION;

    /* get self address */
    margo_instance_id mid = args->mid;
    ctx->mid = mid;
    hg_addr_t self_address;
    char      self_address_string[256];
    hg_size_t self_address_size = 256;
    hg_return_t hret = margo_addr_self(mid, &self_address);
    if(hret != HG_SUCCESS) {
        ret = FLOCK_ERR_FROM_MERCURY;
        goto error;
    }
    hret = margo_addr_to_string(mid, self_address_string, &self_address_size, self_address);
    margo_addr_free(mid, self_address);
    if(hret != HG_SUCCESS) {
        ret = FLOCK_ERR_FROM_MERCURY;
        goto error;
    }

    /* check who is the primary member */
    flock_member_t* primary_member = &args->initial_view.members.data[0];
    ctx->is_primary = (primary_member->provider_id == args->provider_id)
                   && (strcmp(primary_member->address, self_address_string) == 0);

    /* lookup primary member's address and provider ID */
    hret = margo_addr_lookup(mid, primary_member->address, &ctx->primary.address);
    if(hret != HG_SUCCESS) {
        ret = FLOCK_ERR_FROM_MERCURY;
        goto error;
    }
    ctx->primary.provider_id = primary_member->provider_id;

    /* copy the configuration */
    ctx->rank                  = args->rank;
    ctx->config                = config;
    ctx->ping_timeout_ms       = ping_timeout_ms_val;
    ctx->ping_interval_ms_min  = ping_interval_ms_min;
    ctx->ping_interval_ms_max  = ping_interval_ms_max;
    ctx->ping_max_num_timeouts = ping_max_num_timeouts;

    /* copy the membership and metadata update callbask */
    ctx->member_update_callback   = args->member_update_callback;
    ctx->metadata_update_callback = args->metadata_update_callback;
    ctx->callback_context         = args->callback_context;

    /* move the initial view in the context */
    FLOCK_GROUP_VIEW_MOVE(&args->initial_view, &ctx->view);

    /* register the RPCs */
    ctx->ping_rpc_id = MARGO_REGISTER_PROVIDER(
        mid, "flock_centralized_ping",
        uint64_t, void, ping_rpc_ult,
        args->provider_id, args->pool);
    margo_register_data(mid, ctx->ping_rpc_id, ctx, NULL);

    ctx->membership_update_rpc_id = MARGO_REGISTER_PROVIDER(
        mid, "flock_centralized_membership_update",
        membership_update_in_t, membership_update_out_t,
        membership_update_rpc_ult, args->provider_id, args->pool);
    margo_register_data(mid, ctx->membership_update_rpc_id, ctx, NULL);

    ctx->get_view_rpc_id = MARGO_REGISTER_PROVIDER(
        mid, "flock_centralized_get_view",
        void, flock_protected_group_view_t, get_view_rpc_ult,
        args->provider_id, args->pool);
    margo_register_data(mid, ctx->get_view_rpc_id, ctx, NULL);

    ctx->leave_rpc_id = MARGO_REGISTER_PROVIDER(
        mid, "flock_centralized_leave",
        uint64_t, void, leave_rpc_ult,
        args->provider_id, args->pool);
    margo_register_data(mid, ctx->leave_rpc_id, ctx, NULL);

    if(ctx->is_primary) {
        for(size_t i = 0; i < ctx->view.members.size; ++i) {
            flock_member_t* member = &ctx->view.members.data[i];
            member->extra.data  = calloc(1, sizeof(struct member_state));
            member->extra.free  = member_state_free;
            member_state* state = (member_state*)(member->extra.data);
            state->context      = ctx;
            state->owner        = member;
            if(HG_SUCCESS != margo_addr_lookup(mid, member->address, &state->address)) {
                ret = FLOCK_ERR_FROM_MERCURY;
                goto error;
            }
            if(i == 0) continue;
            // create timer only for non-primary members
            margo_timer_create(mid, ping_timer_callback, state, &state->ping_timer);
            state->last_ping_timestamp = ABT_get_wtime();
            double interval = RAND_BETWEEN(
                state->context->ping_interval_ms_min,
                state->context->ping_interval_ms_max);
            margo_timer_start(state->ping_timer, interval);
        }
    } else if(args->join) {
        ret = get_view(ctx);
        if(ret != FLOCK_SUCCESS) goto error;
        // TODO
    }

    *context = ctx;
    return FLOCK_SUCCESS;

error:
    centralized_destroy_group(ctx);
    return ret;
}

// -------------------------------------------------------------------------------
// PING MECHANISM
// -------------------------------------------------------------------------------

static void ping_timer_callback(void* args)
{
    double now, next_ping_ms;
    member_state* state = (member_state*)args;
    state->in_timer_callback = true;
    hg_return_t hret = HG_SUCCESS;

    ABT_mutex_lock(ABT_MUTEX_MEMORY_GET_HANDLE(&state->mtx));
    hret = margo_create(
            state->context->mid,
            state->address,
            state->context->ping_rpc_id,
            &state->last_ping_handle);
    if(hret != HG_SUCCESS) {
        margo_warning(state->context->mid, "[flock] Failed to create ping RPC handle");
        ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&state->mtx));
        goto restart_timer;
    }
    margo_request req = MARGO_REQUEST_NULL;
    double timeout = state->context->ping_timeout_ms;
    hret = margo_provider_iforward_timed(
            state->owner->provider_id,
            state->last_ping_handle,
            &state->context->view.digest,
            timeout, &req);
    if(hret != HG_SUCCESS) {
        margo_warning(state->context->mid, "[flock] Failed to forward ping RPC handle");
        ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&state->mtx));
        goto restart_timer;
    }
    ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&state->mtx));

    // wait for request outside of lock, since the handle can be cancelled
    hret = margo_wait(req);

    ABT_mutex_lock(ABT_MUTEX_MEMORY_GET_HANDLE(&state->mtx));
    margo_destroy(state->last_ping_handle);
    state->last_ping_handle = HG_HANDLE_NULL;
    ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&state->mtx));

    // request was canceled, we need to terminate
    if(hret == HG_CANCELED) {
        state->in_timer_callback = false;
        return;
    }

    if(hret == HG_SUCCESS) state->num_ping_timeouts = 0;
    else state->num_ping_timeouts += 1;

    centralized_context* context = state->context;
    if(state->num_ping_timeouts == context->ping_max_num_timeouts) {
        size_t rank                  = state->owner->rank;
        char* address                = strdup(state->owner->address);
        uint16_t provider_id         = state->owner->provider_id;
        margo_trace(context->mid,
            "[flock] Ping to member %lu (%s, %d) timed out %d times, "
            "considering the member dead.", rank, address, provider_id, context->ping_max_num_timeouts);
        FLOCK_GROUP_VIEW_LOCK(&context->view);
        flock_group_view_remove_member(&context->view, rank);
        FLOCK_GROUP_VIEW_UNLOCK(&context->view);

        if(context->member_update_callback) {
            (context->member_update_callback)(
                context->callback_context, FLOCK_MEMBER_DIED, rank, address, provider_id);
        }

        broadcast_membership_update(context, FLOCK_MEMBER_DIED, rank, address, provider_id);
        free(address);
        return;
    }

restart_timer:
    state->in_timer_callback = false;
    now = ABT_get_wtime();
    next_ping_ms = RAND_BETWEEN(
        state->context->ping_interval_ms_min,
        state->context->ping_interval_ms_max);
    next_ping_ms -= (now - state->last_ping_timestamp)*1000.0;
    state->last_ping_timestamp = now;
    if(next_ping_ms <= 0) next_ping_ms = 1.0;
    ABT_mutex_lock(ABT_MUTEX_MEMORY_GET_HANDLE(&state->mtx));
    if(state->ping_timer) {
        margo_timer_start(state->ping_timer, next_ping_ms);
    }
    ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&state->mtx));
}

static DEFINE_MARGO_RPC_HANDLER(ping_rpc_ult)
static void ping_rpc_ult(hg_handle_t h)
{
    margo_instance_id mid = margo_hg_handle_get_instance(h);
    const struct hg_info* info = margo_get_info(h);
    centralized_context* ctx = margo_registered_data(mid, info->id);

    uint64_t digest = 0;
    margo_get_input(h, &digest);

    margo_request req = MARGO_REQUEST_NULL;
    margo_irespond(h, NULL, &req);

    if(digest == ctx->view.digest) {
        goto finish;
    }

    get_view(ctx);

finish:
    margo_wait(req);
    margo_free_input(h, &digest);
    margo_destroy(h);
}

// -------------------------------------------------------------------------------
// GET_VIEW (RPC send by non-zero ranks to primary)
// -------------------------------------------------------------------------------

static DEFINE_MARGO_RPC_HANDLER(get_view_rpc_ult)
static void get_view_rpc_ult(hg_handle_t h)
{
    /* find the margo instance */
    margo_instance_id mid = margo_hg_handle_get_instance(h);

    /* find the context */
    const struct hg_info* info = margo_get_info(h);
    centralized_context* ctx = (centralized_context*)margo_registered_data(mid, info->id);
    if(!ctx) goto finish;

    /* respond with the current view */
    margo_respond(h, &ctx->view);

finish:
    margo_destroy(h);
}

static flock_return_t get_view(centralized_context* ctx)
{
    hg_return_t hret   = HG_SUCCESS;
    hg_handle_t handle = HG_HANDLE_NULL;
    hret = margo_create(ctx->mid, ctx->primary.address, ctx->get_view_rpc_id, &handle);
    if(hret != HG_SUCCESS) goto finish;

    hret = margo_provider_forward(ctx->primary.provider_id, handle, NULL);
    if(hret != HG_SUCCESS) {
        // LCOV_EXCL_START
        margo_error(ctx->mid,
            "[flock] Could not get view from primary member, "
            "margo_provider_forward failed: %s", HG_Error_to_string(hret));
        goto finish;
        // LCOV_EXCL_STOP
    }

    hret = margo_get_output(handle, &ctx->view);
    if(hret != HG_SUCCESS) {
        // LCOV_EXCL_START
        margo_error(ctx->mid,
            "[flock] Could not get view from primary member, "
            "margo_get_output failed: %s", HG_Error_to_string(hret));
        goto finish;
        // LCOV_EXCL_STOP
    }

    margo_free_output(handle, NULL);

finish:
    if(handle) margo_destroy(handle);
    return FLOCK_SUCCESS;
}

// -------------------------------------------------------------------------------
// LEAVE (RPC send by a non-zero rank to primary when it wants to leave)
// -------------------------------------------------------------------------------

static DEFINE_MARGO_RPC_HANDLER(leave_rpc_ult)
static void leave_rpc_ult(hg_handle_t h)
{
    char* address = NULL;
    uint16_t provider_id;
    uint64_t rank;

    /* find the margo instance */
    margo_instance_id mid = margo_hg_handle_get_instance(h);

    /* find the context */
    const struct hg_info* info = margo_get_info(h);
    centralized_context* ctx = (centralized_context*)margo_registered_data(mid, info->id);
    if(!ctx) goto finish;

    /* the leaving provider sent its rank */
    hg_return_t hret = margo_get_input(h, &rank);
    if(hret != HG_SUCCESS) {
        // LCOV_EXCL_START
        margo_error(ctx->mid, "[flock] Could not deserialize rank of leaving process");
        goto finish;
        // LCOV_EXCL_STOP
    }
    margo_free_input(h, &rank);

    FLOCK_GROUP_VIEW_LOCK(&ctx->view);
    const flock_member_t* member = flock_group_view_find_member(&ctx->view, rank);
    if(!member) {
        FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);
        margo_error(ctx->mid, "[flock] Rank %lu requested to leave but is not part of the group");
        goto finish;
    }
    provider_id = member->provider_id;
    address     = strdup(member->address);
    flock_group_view_remove_member(&ctx->view, rank);
    FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);

    if(ctx->member_update_callback) {
        (ctx->member_update_callback)(
            ctx->callback_context, FLOCK_MEMBER_LEFT, rank, address, provider_id);
    }

    broadcast_membership_update(ctx, FLOCK_MEMBER_LEFT, rank, address, provider_id);

finish:
    free(address);
    margo_respond(h, NULL);
    margo_destroy(h);
}

static flock_return_t leave(centralized_context* ctx)
{
    hg_return_t hret   = HG_SUCCESS;
    hg_handle_t handle = HG_HANDLE_NULL;
    hret = margo_create(ctx->mid, ctx->primary.address, ctx->leave_rpc_id, &handle);
    if(hret != HG_SUCCESS) goto finish;

    margo_provider_forward_timed(ctx->primary.provider_id, handle, &ctx->rank, 1000.0);

finish:
    if(handle) margo_destroy(handle);
    return FLOCK_SUCCESS;
}

// -------------------------------------------------------------------------------
// MEMBERSHIP UPDATE
// (RPC send by rank 0 to other ranks to notify them of a membership change)
// -------------------------------------------------------------------------------

static flock_return_t broadcast_membership_update(
    centralized_context* ctx,
    flock_update_t update,
    size_t rank, const char* address,
    uint16_t provider_id)
{
    hg_return_t  hret       = HG_SUCCESS;
    flock_return_t ret      = FLOCK_SUCCESS;
    size_t       rpc_count  = 0;
    hg_handle_t* handles    = NULL;
    margo_request* requests = NULL;
    membership_update_out_t* out;
    membership_update_in_t in = {
        .update      = (uint8_t)update,
        .rank        = rank,
        .address     = address,
        .provider_id = provider_id
    };

    FLOCK_GROUP_VIEW_LOCK(&ctx->view);
    rpc_count = ctx->view.members.size - 1;
    handles   = (hg_handle_t*)calloc(rpc_count, sizeof(*handles));
    requests  = (margo_request*)calloc(rpc_count, sizeof(*requests));
    out       = (membership_update_out_t*)calloc(rpc_count, sizeof(*out));

    margo_trace(ctx->mid,
        "[flock] Issuing membership update to %lu members...", rpc_count);

    for(size_t i = 0; i < rpc_count; ++i) {
        member_state* state = (member_state*)ctx->view.members.data[i+1].extra.data;
        hret = margo_create(ctx->mid, state->address, ctx->membership_update_rpc_id, &handles[i]);
        if(hret != HG_SUCCESS) {
            margo_error(ctx->mid,
                "[flock] Could not create handle to issue membership update to rank %lu",
                state->owner->rank);
        }
    }

    for(size_t i = 0; i < rpc_count; ++i) {
        member_state* state = (member_state*)ctx->view.members.data[i+1].extra.data;
        if(!handles[i]) continue;
        hret = margo_provider_iforward_timed(
            state->owner->provider_id, handles[i], &in, 1000.0, &requests[i]);
        if(hret != HG_SUCCESS) {
            margo_error(ctx->mid,
                "[flock] Could not forward membership update to rank %lu",
                state->owner->rank);
        }
    }
    FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);

    for(size_t i = 0; i < rpc_count; ++i) {
        if(requests[i]) {
            margo_wait(requests[i]);
            margo_get_output(handles[i], &out[i]);
            margo_free_output(handles[i], &out[i]);
        }
        if(handles[i]) margo_destroy(handles[i]);
    }
    free(requests);
    free(handles);
    free(out);

    return ret;
}

static DEFINE_MARGO_RPC_HANDLER(membership_update_rpc_ult)
static void membership_update_rpc_ult(hg_handle_t h)
{
    hg_return_t hret = HG_SUCCESS;
    membership_update_in_t in = {0};
    membership_update_out_t out = {0};

    /* find the margo instance */
    margo_instance_id mid = margo_hg_handle_get_instance(h);

    /* find the context */
    const struct hg_info* info = margo_get_info(h);
    centralized_context* ctx = (centralized_context*)margo_registered_data(mid, info->id);
    if(!ctx) goto finish;

    hret = margo_get_input(h, &in);
    if(hret != HG_SUCCESS) {
        // LCOV_EXCL_START
        out.ret = (uint32_t)FLOCK_ERR_FROM_MERCURY;
        goto finish;
        // LCOV_EXCL_STOP
    }

    FLOCK_GROUP_VIEW_LOCK(&ctx->view);
    flock_group_view_remove_member(&ctx->view, in.rank);
    FLOCK_GROUP_VIEW_UNLOCK(&ctx->view);

    if(ctx->member_update_callback) {
        (ctx->member_update_callback)(
            ctx->callback_context, in.update, in.rank, in.address, in.provider_id);
    }

    /* respond with the current view */
    margo_respond(h, &ctx->view);

finish:
    margo_free_input(h, &in);
    margo_respond(h, &out);
    margo_destroy(h);
}

static flock_return_t centralized_destroy_group(void* ctx)
{
    centralized_context* context = (centralized_context*)ctx;
    if(context->is_primary) {
        // Terminate the ULTs that ping the other members
        // We do this before deregistering the RPCs to avoid ULTs
        // failing because they are still using the RPC ids.
        FLOCK_GROUP_VIEW_LOCK(&context->view);
        margo_timer_t* timers = (margo_timer_t*)calloc(context->view.members.size, sizeof(*timers));
        size_t num_timers = 0;
        for(size_t i = 0; i < context->view.members.size; ++i) {
            member_state* state = (member_state*)context->view.members.data[i].extra.data;
            if(!state) continue;
            if(state->ping_timer == MARGO_TIMER_NULL) continue;
            timers[num_timers] = state->ping_timer;
            num_timers += 1;
        }
        margo_timer_cancel_many(num_timers, timers);
        free(timers);
        flock_group_view_clear_extra(&context->view);
        FLOCK_GROUP_VIEW_UNLOCK(&context->view);
    } else {
        leave(ctx);
    }
    // FIXME: in non-primary members, it's possible that we are calling margo_deregister
    // while a ping RPC is in flight. There is nothing much we can do, this is something
    // to solve at the Mercury level. See here: https://github.com/mercury-hpc/mercury/issues/534
    if(context->ping_rpc_id) margo_deregister(context->mid, context->ping_rpc_id);
    if(context->get_view_rpc_id) margo_deregister(context->mid, context->get_view_rpc_id);
    if(context->membership_update_rpc_id) margo_deregister(context->mid, context->membership_update_rpc_id);
    if(context->leave_rpc_id) margo_deregister(context->mid, context->leave_rpc_id);
    if(context->config) json_object_put(context->config);
    flock_group_view_clear(&context->view);
    free(context);
    return FLOCK_SUCCESS;
}

static flock_return_t centralized_get_config(
    void* ctx, void (*fn)(void*, const struct json_object*), void* uargs)
{
    centralized_context* context = (centralized_context*)ctx;
    ABT_mutex_lock(ABT_MUTEX_MEMORY_GET_HANDLE(&context->config_mtx));
    fn(uargs, context->config);
    ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&context->config_mtx));
    return FLOCK_SUCCESS;
}

static flock_return_t centralized_get_view(
    void* ctx, void (*fn)(void*, const flock_group_view_t* view), void* uargs)
{
    centralized_context* context = (centralized_context*)ctx;
    fn(uargs, &context->view);
    return FLOCK_SUCCESS;
}

static flock_return_t centralized_add_metadata(
        void* ctx, const char* key, const char* value)
{
    // LCOV_EXCL_START
    (void)ctx;
    (void)key;
    (void)value;
    return FLOCK_ERR_OP_UNSUPPORTED;
    // LCOV_EXCL_STOP
}

static flock_return_t centralized_remove_metadata(
        void* ctx, const char* key)
{
    // LCOV_EXCL_START
    (void)ctx;
    (void)key;
    return FLOCK_ERR_OP_UNSUPPORTED;
    // LCOV_EXCL_STOP
}

static flock_backend_impl centralized_backend = {
    .name            = "centralized",
    .init_group      = centralized_create_group,
    .destroy_group   = centralized_destroy_group,
    .get_config      = centralized_get_config,
    .get_view        = centralized_get_view,
    .add_metadata    = centralized_add_metadata,
    .remove_metadata = centralized_remove_metadata
};

flock_return_t flock_register_centralized_backend(void)
{
    return flock_register_backend(&centralized_backend);
}
