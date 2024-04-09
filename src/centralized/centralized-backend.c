/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <string.h>
#include <json-c/json.h>
#include <margo-timer.h>
#include "flock/flock-backend.h"
#include "flock/flock-group-view.h"
#include "../provider.h"
#include "centralized-backend.h"

/**
 * @brief The "centralized" backend uses the member with the lowest rank
 * as a centralized authority that is supposed to hold the most up to date
 * group view. The flock_group_view_t in all the other processes is a
 * read-only, cached version. The primary member will ping the secondary
 * members periodically to check that they are alive.
 */
typedef struct centralized_context {
    margo_instance_id    mid;
    struct json_object*  config;
    ABT_mutex_memory     config_mtx;
    bool                 is_primary;
    struct {
        hg_addr_t        address;
        uint16_t         provider_id;
    } primary;
    flock_group_view_t   view;
    hg_id_t              ping_rpc_id;
    hg_id_t              get_view_rpc_id;
} centralized_context;

/**
 * @brief Extra data attached to each member in the view in the primary member.
 */
typedef struct member_state {
    centralized_context* context;
    hg_addr_t            address;
    uint16_t             provider_id;
    margo_timer_t        timer;
    hg_handle_t          last_ping_handle;
    uint8_t              num_ping_timeouts;
    _Atomic bool         need_stop;
} member_state;

static inline void member_state_free(void* args)
{
    member_state* state = (member_state*)args;
    state->need_stop = true;
    if(state->last_ping_handle) {
        HG_Cancel(state->last_ping_handle);
    }
    if(state->timer) {
        margo_timer_cancel(state->timer);
        margo_timer_destroy(state->timer);
        state->timer = MARGO_TIMER_NULL;
    }
    margo_addr_free(state->context->mid, state->address);
    free(state);
}

static DECLARE_MARGO_RPC_HANDLER(ping_rpc_ult)
static void ping_rpc_ult(hg_handle_t h);

static void ping_timer_callback(void* args);

static DECLARE_MARGO_RPC_HANDLER(get_view_rpc_ult)
static void get_view_rpc_ult(hg_handle_t h);

static flock_return_t centralized_destroy_group(void* ctx);

static flock_return_t centralized_create_group(
        flock_backend_init_args_t* args,
        void** context)
{
    flock_return_t ret = FLOCK_SUCCESS;

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
    // TODO
    ctx->config = json_object_new_object();

    /* move the initial view in the context */
    FLOCK_GROUP_VIEW_MOVE(&args->initial_view, &ctx->view);

    /* register the RPCs */
    ctx->ping_rpc_id = MARGO_REGISTER_PROVIDER(
        mid, "flock_centralized_ping", uint64_t, void, ping_rpc_ult,
        args->provider_id, args->pool);
    margo_register_data(mid, ctx->ping_rpc_id, ctx, NULL);

    ctx->get_view_rpc_id = MARGO_REGISTER_PROVIDER(
        mid, "flock_centralized_get_view", void, flock_protected_group_view_t, get_view_rpc_ult,
        args->provider_id, args->pool);
    margo_register_data(mid, ctx->get_view_rpc_id, ctx, NULL);

    if(ctx->is_primary) {
        // Note: the loop starts at 1 because the first member is the
        // primary member and is not going to ping itself
        for(size_t i = 1; i < ctx->view.members.size; ++i) {
            flock_member_t* member = &ctx->view.members.data[i];
            member->extra.data  = calloc(1, sizeof(struct member_state));
            member->extra.free  = member_state_free;
            member_state* state = (member_state*)(member->extra.data);
            state->context      = ctx;
            state->provider_id  = member->provider_id;
            if(HG_SUCCESS != margo_addr_lookup(mid, member->address, &state->address)) {
                ret = FLOCK_ERR_FROM_MERCURY;
                goto error;
            }
            margo_timer_create(mid, ping_timer_callback, state, &state->timer);
            margo_timer_start(state->timer, 1000);
        }
    }

    *context = ctx;
    return FLOCK_SUCCESS;

error:
    centralized_destroy_group(ctx);
    return ret;
}

static void ping_timer_callback(void* args)
{
    member_state* state = (member_state*)args;
    if(state->need_stop) return;

    hg_return_t hret = HG_SUCCESS;
    hret = margo_create(
            state->context->mid,
            state->address,
            state->context->ping_rpc_id,
            &state->last_ping_handle);
    if(hret != HG_SUCCESS) {
        // TODO print an error
        return;
    }
    double t1 = ABT_get_wtime();
    hret = margo_provider_forward_timed(
            state->provider_id,
            state->last_ping_handle,
            &state->context->view.digest,
            1000);
    double t2 = ABT_get_wtime();
    margo_destroy(state->last_ping_handle);
    state->last_ping_handle = HG_HANDLE_NULL;

    double next_ms = 1000.0 - (t2 - t1)*1000.0;
    if(next_ms <= 0) next_ms = 1000.0;

    switch(hret) {
        case HG_SUCCESS:
            state->num_ping_timeouts = 0;
            margo_timer_start(state->timer, next_ms);
            return;
        case HG_TIMEOUT:
            state->num_ping_timeouts += 1;
            margo_timer_start(state->timer, next_ms);
            return;
        default:
            return;
    }

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

    hg_return_t hret   = HG_SUCCESS;
    hg_handle_t handle = HG_HANDLE_NULL;
    hret = margo_create(ctx->mid, ctx->primary.address, ctx->get_view_rpc_id, &handle);

    hret = margo_provider_forward(ctx->primary.provider_id, handle, NULL);

    hret = margo_get_output(handle, &ctx->view);

    margo_free_output(handle, NULL);
    margo_destroy(handle);

finish:
    margo_wait(req);
    margo_free_input(h, &digest);
    margo_destroy(h);
}

static DEFINE_MARGO_RPC_HANDLER(get_view_rpc_ult)
static void get_view_rpc_ult(hg_handle_t h)
{
    /* find the margo instance */
    margo_instance_id mid = margo_hg_handle_get_instance(h);

    /* find the context */
    const struct hg_info* info = margo_get_info(h);
    centralized_context* ctx = (centralized_context*)margo_registered_data(mid, info->id);

    /* respond with the current view */
    margo_respond(h, &ctx->view);

    margo_destroy(h);
}

static flock_return_t centralized_destroy_group(void* ctx)
{
    centralized_context* context = (centralized_context*)ctx;
    if(context->ping_rpc_id) margo_deregister(context->mid, context->ping_rpc_id);
    if(context->get_view_rpc_id) margo_deregister(context->mid, context->get_view_rpc_id);
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

static flock_return_t centralized_add_member(
    void* ctx, uint64_t rank, const char* address, uint16_t provider_id)
{
    // LCOV_EXCL_START
    (void)ctx;
    (void)rank;
    (void)address;
    (void)provider_id;
    return FLOCK_ERR_OP_UNSUPPORTED;
    // LCOV_EXCL_END
}

static flock_return_t centralized_remove_member(
    void* ctx, uint64_t rank)
{
    // LCOV_EXCL_START
    (void)ctx;
    (void)rank;
    return FLOCK_ERR_OP_UNSUPPORTED;
    // LCOV_EXCL_END
}

static flock_return_t centralized_add_metadata(
    void* ctx, const char* key, const char* value)
{
    // LCOV_EXCL_START
    (void)ctx;
    (void)key;
    (void)value;
    return FLOCK_ERR_OP_UNSUPPORTED;
    // LCOV_EXCL_END
}

static flock_return_t centralized_remove_metadata(
    void* ctx, const char* key)
{
    // LCOV_EXCL_START
    (void)ctx;
    (void)key;
    return FLOCK_ERR_OP_UNSUPPORTED;
    // LCOV_EXCL_END
}

static flock_backend_impl centralized_backend = {
    .name               = "centralized",
    .init_group         = centralized_create_group,
    .destroy_group      = centralized_destroy_group,
    .get_config         = centralized_get_config,
    .get_view           = centralized_get_view,
    .add_member         = centralized_add_member,
    .remove_member      = centralized_remove_member,
    .add_metadata       = centralized_add_metadata,
    .remove_metadata    = centralized_remove_metadata
};

flock_return_t flock_register_centralized_backend(void)
{
    return flock_register_backend(&centralized_backend);
}
